
// 将table1读入内存，建立index。建立unordered_multimap，key为id3，value为Row*。
// 如果table1比table2大，那么交换两个file_name。是为了在内存中建立比较小的那个表的map。

// 然后顺序扫描table2，若t2.id3存在table1的index中，则将pair<Row*,Row*>加入join的结果vector中。后续不变。运行时间776毫秒。下降了。
// 第二种方法。顺序扫描table2，但是后续不使用sort进行group。而是采用map，key为group by的两个值组成的pair，value为Group*。这样table2的row如果和table1的row符合join谓语，那么这两个row是会放在一个group中的，直接计算是哪一个group（map key），更新Group*里面的max(t1.id1), min(t2.id1)。649毫秒，还是没有直接sort的结果好。

// 最后还是将Group*序列按照t1_id1进行排序。因为Group* 已经按照group by 的那两个列排完序了。

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

#define DEBUG

struct Row {
    int id1;
    int id2;
    int id3;
};

struct Group {
    int t1_id1;
    int t2_id1;
};

struct SortByComparator {
    bool operator()(const Group *group1, const Group *group2) const {
        return group1->t1_id1 < group2->t1_id1;
    }
};

std::unordered_multimap<int, Row *> read_rows(std::string filename) {
    std::unordered_multimap<int, Row *> res;
    std::ifstream fin(filename);
    char c;
    while (!fin.eof()) {
        auto row = new Row();
        if (fin >> row->id1 >> c >> row->id2 >> c >> row->id3) {
            res.insert({row->id3, row});
        } else {
            break;
        }
    }
    return res;
}

struct GroupComparator {
    bool operator()(const std::pair<int, int> &p1, const std::pair<int, int> &p2) const {
        if (p1.second != p2.second) {
            return p1.second < p2.second;
        } else {
            return p1.first < p2.first;
        }
    }
};

std::map<std::pair<int, int>, Group *, GroupComparator> join(std::string file1_name, std::string file2_name) {
#ifdef DEBUG
    auto read_rows_begin = std::chrono::high_resolution_clock::now();
#endif
    auto t1_rows_map = read_rows(file1_name);

#ifdef DEBUG
    auto read_rows_end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(read_rows_end - read_rows_begin);
    std::cout << "-----read_rows: "
              << duration.count() << " milliseconds" << std::endl;
#endif

#ifdef DEBUG
    auto merge_join_begin = std::chrono::high_resolution_clock::now();
#endif

    std::map<std::pair<int, int>, Group *, GroupComparator> group_map;
    std::ifstream fin2(file2_name);
    char c;
    int t2_id1, t2_id2, t2_id3;
    while (!fin2.eof()) {
        if (fin2 >> t2_id1 >> c >> t2_id2 >> c >> t2_id3) {
            auto range = t1_rows_map.equal_range(t2_id3);
            if (range.first != range.second) {
                for (auto range_it = range.first; range_it != range.second; range_it++) {
                    auto find_it = group_map.find(std::make_pair(range_it->second->id2, t2_id2));
                    if (find_it == group_map.end()) {
                        auto group = new Group();
                        group->t1_id1 = range_it->second->id1;
                        group->t2_id1 = t2_id1;
                        group_map.insert({std::make_pair(range_it->second->id2, t2_id2), group});
                    } else {
                        find_it->second->t1_id1 = std::max(find_it->second->t1_id1, range_it->second->id1);
                        find_it->second->t2_id1 = std::min(find_it->second->t2_id1, t2_id1);
                    }
                }
            }
        } else {
            break;
        }
    }

#ifdef DEBUG
    auto merge_join_end = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::milliseconds>(merge_join_end - merge_join_begin);
    std::cout << "-----merge join: "
              << duration.count() << " milliseconds" << std::endl;
#endif
    return group_map;
}

std::vector<Group *> group(std::map<std::pair<int, int>, Group *, GroupComparator> &group_map) {
    std::vector<Group *> groups;
    for (auto it = group_map.begin(); it != group_map.end(); it++) {
        groups.push_back((*it).second);
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
    std::ifstream fin1(file1_name, std::ios::ate);
    std::ifstream fin2(file2_name, std::ios::ate);
    if (fin1.tellg() > fin2.tellg()) {
        std::swap(file1_name, file2_name);
    }
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
