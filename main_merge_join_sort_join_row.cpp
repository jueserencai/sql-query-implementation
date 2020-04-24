// sort merge join
// sort join row by t2_id2,t1_id2, then group
// group 已经按照 t2_id2,t1_id2 排序了，所以只用在 max(t1_id1) 上稳定排序就行

// 占用内存很低，因为每个操作只把需要的字段存在内存，和当前操作无关的不读入内存。 但是需要读磁盘多次，所以速度较慢。
// 结果为 2300毫秒
// 已经在 group 读入id2的时候预先读入id1, 不用read_id1，减少读磁盘次数。 结果变为 1436毫秒
// 预先读入两个table的所有row，再join等等。只有一遍读磁盘操作，所以速度更快了。变成 615毫秒

// 里面涉及了三次排序，可能是需要改进的地方

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// #define DEBUG

using JoinColmunType = int;

struct Row {
    int id1;
    int id2;
    int id3;
};

struct Group {
    int t1_id1;
    int t2_id1;
};

void advance(std::vector<Row *>::iterator &it_begin,
             std::vector<Row *>::iterator &it_end,
             const std::vector<Row *>::iterator &vector_it_end) {
    it_begin = it_end;
    if (it_begin == vector_it_end)
        return;
    JoinColmunType key = (*it_begin)->id3;
    while (it_end != vector_it_end && (*it_end)->id3 == key) {
        it_end++;
    }
}

struct SortMergeComparator {
    bool operator()(const Row *row1, const Row *row2) const {
        return row1->id3 < row2->id3;
    }
};

std::vector<std::pair<Row *, Row *>> sort_merge_join(std::vector<Row *> &left_relation,
                                                     std::vector<Row *> &right_relation) {
    std::vector<std::pair<Row *, Row *>> output;
    std::sort(left_relation.begin(), left_relation.end(), SortMergeComparator());
    std::sort(right_relation.begin(), right_relation.end(), SortMergeComparator());

    auto &left_sorted = left_relation;
    auto &right_sorted = right_relation;

    auto left_it_begin = left_sorted.begin();
    auto left_it_end = left_sorted.begin();
    auto right_it_begin = right_sorted.begin();
    auto right_it_end = right_sorted.begin();
    auto left_sorted_end = left_sorted.end();
    auto right_sorted_end = right_sorted.end();

    advance(left_it_begin, left_it_end, left_sorted_end);
    advance(right_it_begin, right_it_end, right_sorted_end);

    while (left_it_begin != left_it_end && right_it_begin != right_it_end) {
        JoinColmunType left_key = (*left_it_begin)->id3;
        JoinColmunType right_key = (*right_it_begin)->id3;

        if (left_key == right_key) {
            for (auto left_it = left_it_begin; left_it != left_it_end; left_it++) {
                for (auto right_it = right_it_begin; right_it != right_it_end; right_it++) {
                    output.push_back(std::make_pair((*left_it), (*right_it)));
                }
            }
            advance(left_it_begin, left_it_end, left_sorted_end);
            advance(right_it_begin, right_it_end, right_sorted_end);
        } else if (left_key < right_key) {
            advance(left_it_begin, left_it_end, left_sorted_end);
        } else {
            advance(right_it_begin, right_it_end, right_sorted_end);
        }
    }
    output.resize(output.size());
    return output;
}

struct GroupBySortComparator {
    bool operator()(const std::pair<Row *, Row *> &pair1, const std::pair<Row *, Row *> &pair2) const {
        if (pair1.second->id2 != pair2.second->id2) {
            return pair1.second->id2 < pair2.second->id2;
        } else {
            return pair1.first->id2 < pair2.first->id2;
        }
    }
};
struct SortByComparator {
    bool operator()(const Group *group1, const Group *group2) const {
        return group1->t1_id1 < group2->t1_id1;
    }
};

std::vector<Row *> read_rows(std::string filename) {
    std::vector<Row *> res;
    std::ifstream fin(filename);
    char c;
    while (!fin.eof()) {
        auto row = new Row();
        if (fin >> row->id1 >> c >> row->id2 >> c >> row->id3) {
            res.push_back(row);
        } else {
            break;
        }
    }
    return res;
}

std::vector<std::pair<Row *, Row *>> join(std::string file1_name, std::string file2_name) {
#ifdef DEBUG
    auto read_rows_begin = std::chrono::high_resolution_clock::now();
#endif
    std::vector<Row *> t1_rows = read_rows(file1_name);
    std::vector<Row *> t2_rows = read_rows(file2_name);

#ifdef DEBUG
    auto read_rows_end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(read_rows_end - read_rows_begin);
    std::cout << "-----read_rows: "
              << duration.count() << " milliseconds" << std::endl;
#endif

#ifdef DEBUG
    auto merge_join_begin = std::chrono::high_resolution_clock::now();
#endif
    std::vector<std::pair<Row *, Row *>> join_rows = sort_merge_join(t1_rows, t2_rows);

#ifdef DEBUG
    auto merge_join_end = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::milliseconds>(merge_join_end - merge_join_begin);
    std::cout << "-----merge join: "
              << duration.count() << " milliseconds" << std::endl;
#endif
    return join_rows;
}

std::vector<Group *> group(std::vector<std::pair<Row *, Row *>> &join_rows) {
    std::sort(join_rows.begin(), join_rows.end(), GroupBySortComparator());
    std::vector<Group *> groups;

    // 读入第一个group row
    auto group = new Group();
    group->t1_id1 = join_rows[0].first->id1;
    group->t2_id1 = join_rows[0].second->id1;
    groups.push_back(group);
    int group_by_key1_last = join_rows[0].first->id2, group_by_key2_last = join_rows[0].second->id2;

    // 后续group row
    int group_by_key1, group_by_key2;
    int t1_id1, t2_id1;
    for (auto row_it = join_rows.begin() + 1; row_it != join_rows.end(); row_it++) {
        group_by_key1 = (*row_it).first->id2;
        group_by_key2 = (*row_it).second->id2;
        if (group_by_key1 == group_by_key1_last && group_by_key2 == group_by_key2_last) {  // 同一个group
            auto &group = groups.back();
            group->t1_id1 = std::max(group->t1_id1, (*row_it).first->id1);
            group->t2_id1 = std::min(group->t2_id1, (*row_it).second->id1);
        } else {  // 不同group
            auto group = new Group();
            group->t1_id1 = (*row_it).first->id1;
            group->t2_id1 = (*row_it).second->id1;
            groups.push_back(group);

            group_by_key1_last = group_by_key1;
            group_by_key2_last = group_by_key2;
        }
    }
    return groups;
}

int main() {
#ifdef DEBUG
    std::string file1_name = "input3.csv";
    std::string file2_name = "input4.csv";
#else
    std::string file1_name = "/home/web/ztedatabase/input1.csv";
    std::string file2_name = "/home/web/ztedatabase/input2.csv";
#endif

#ifdef DEBUG
    auto join_begin = std::chrono::high_resolution_clock::now();
#endif
    // join
    auto join_rows = join(file1_name, file2_name);

#ifdef DEBUG
    auto join_end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(join_end - join_begin);
    std::cout << "Time join: "
              << duration.count() << " milliseconds" << std::endl;
#endif

#ifdef DEBUG
    auto group_begin = std::chrono::high_resolution_clock::now();
#endif
    // group
    auto groups = group(join_rows);

#ifdef DEBUG
    auto group_end = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::milliseconds>(group_end - group_begin);
    std::cout << "Time group: "
              << duration.count() << " milliseconds" << std::endl;
#endif

#ifdef DEBUG
    auto sort_begin = std::chrono::high_resolution_clock::now();
#endif
    // sort
    std::stable_sort(groups.begin(), groups.end(), SortByComparator());

#ifdef DEBUG
    auto sort_end = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::milliseconds>(sort_end - sort_begin);
    std::cout << "Time sort: "
              << duration.count() << " milliseconds" << std::endl;
#endif

#ifndef DEBUG
    // print result
    for (auto it = groups.begin(); it != groups.end(); it++) {
        std::cout << (*it)->t1_id1 << "," << (*it)->t2_id1 << std::endl;
    }
#endif

    return 0;
}
