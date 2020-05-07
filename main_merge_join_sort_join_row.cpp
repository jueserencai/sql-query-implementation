// sort merge join
// sort join_row by t2_id2,t1_id2, then group
// group 已经按照 t2_id2,t1_id2 排序了，所以只用在 max(t1_id1) 上稳定排序就行

// 第一种。占用内存很低，因为每个操作只把需要的字段存在内存，用文件内偏移位置表示一行记录，和当前操作无关的不读入内存。 但是需要读磁盘多次，所以速度较慢。结果为 2300毫秒
// 第二种。已经在 group 读入id2的时候预先读入id1, 不用read_id1，减少读磁盘次数。 结果变为 1436毫秒
// 第三种。预先读入两个table的所有row，再join等等。只有一遍读磁盘操作，所以速度更快了。变成 615毫秒。多刷几次，最好为588毫秒，最差为680毫秒

// 里面涉及了三次排序，是需要改进的地方。

#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <unordered_map>
#include <vector>

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

// 迭代器前进，确定区间 [it_begin, it_end)
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

// 按照id3排序
struct SortMergeComparator {
    bool operator()(const Row *row1, const Row *row2) const {
        return row1->id3 < row2->id3;
    }
};

// jion. sort merge join.
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

// 从csv文件读取每一行记录
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
    std::vector<Row *> t1_rows = read_rows(file1_name);
    std::vector<Row *> t2_rows = read_rows(file2_name);

    std::vector<std::pair<Row *, Row *>> join_rows = sort_merge_join(t1_rows, t2_rows);

    return join_rows;
}

// 按照 t2.id2, t1.id2 （group by 的反方向） 的顺序排列。因为sql查询语句中sort by 里面有这样的顺序。
struct GroupBySortComparator {
    bool operator()(const std::pair<Row *, Row *> &pair1, const std::pair<Row *, Row *> &pair2) const {
        if (pair1.second->id2 != pair2.second->id2) {
            return pair1.second->id2 < pair2.second->id2;
        } else {
            return pair1.first->id2 < pair2.first->id2;
        }
    }
};

// group. 按照group by的列排序，则同一个group相邻在一起，计算聚集函数。
std::vector<Group *> group(std::vector<std::pair<Row *, Row *>> &join_rows) {
    std::sort(join_rows.begin(), join_rows.end(), GroupBySortComparator());  // 按照 t2.id2, t1.id2 （group by 的反方向） 的顺序排列
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
        if (group_by_key1 == group_by_key1_last && group_by_key2 == group_by_key2_last) {  // 同一个group，更新聚集函数
            auto &group = groups.back();
            group->t1_id1 = std::max(group->t1_id1, (*row_it).first->id1);
            group->t2_id1 = std::min(group->t2_id1, (*row_it).second->id1);
        } else {  // 不同group，添加新group
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

struct SortByComparator {
    bool operator()(const Group *group1, const Group *group2) const {
        return group1->t1_id1 < group2->t1_id1;
    }
};

int main() {
    std::string file1_name = "/home/web/ztedatabase/input1.csv";
    std::string file2_name = "/home/web/ztedatabase/input2.csv";

    auto join_rows = join(file1_name, file2_name);

    auto groups = group(join_rows);

    std::stable_sort(groups.begin(), groups.end(), SortByComparator());  // 按照 max(t1.id1)排序。

    for (auto it = groups.begin(); it != groups.end(); it++) {
        std::cout << (*it)->t1_id1 << "," << (*it)->t2_id1 << std::endl;
    }

    return 0;
}
