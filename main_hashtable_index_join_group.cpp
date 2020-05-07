
// 将table1读入内存，建立index。建立unordered_multimap，key为id3，value为Row*。
// 如果table1比table2大，那么交换两个file_name。是为了在内存中建立比较小的那个表的map。

// 第一种方法。然后顺序扫描table2，若t2.id3存在table1的index中，则将pair<Row*,Row*>加入join的结果vector中。后续不变。运行时间776毫秒。下降了。
// 第二种方法。顺序扫描table2，但是后续不使用sort进行group。而是采用map，key为group by的两个值组成的pair，value为Group*。这样table2的row如果和table1的row符合join谓语，那么这两个row是会放在一个group中的，直接计算是哪一个group（map key），更新Group*里面的max(t1.id1), min(t2.id1)。649毫秒，还是没有直接sort的结果好。

// 最后还是将Group*序列按照t1_id1进行排序。因为Group* 已经按照group by 的那两个列排完序了。

#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

struct Row {
    int id1;
    int id2;
    int id3;
};

struct Group {
    int t1_id1;
    int t2_id1;
};

// 从csv文件读取每一行数据，建立id3的索引。id3可以不用存在Row中。
std::unordered_multimap<int, Row *> read_rows_and_build_index(std::string filename) {
    std::unordered_multimap<int, Row *> res;
    std::ifstream fin(filename);
    char c;
    while (!fin.eof()) {
        auto row = new Row();
        if (fin >> row->id1 >> c >> row->id2 >> c >> row->id3) {
            res.insert({row->id3, row});
        } else {
            delete row;
            break;
        }
    }
    return res;
}

// group key 的比较函数。按照 t2.id2, t1.id2 顺序， sort by的那两个。
struct GroupKeyComparator {
    bool operator()(const std::pair<int, int> &p1, const std::pair<int, int> &p2) const {
        if (p1.second != p2.second) {
            return p1.second < p2.second;
        } else {
            return p1.first < p2.first;
        }
    }
};

std::vector<Group *> join_and_group(std::string file1_name, std::string file2_name) {
    auto t1_rows_index = read_rows_and_build_index(file1_name);

    std::map<std::pair<int, int>, Group *, GroupKeyComparator> group_map;
    std::ifstream fin2(file2_name);
    char c;
    int t2_id1, t2_id2, t2_id3;
    while (!fin2.eof()) {
        if (fin2 >> t2_id1 >> c >> t2_id2 >> c >> t2_id3) {
            auto range = t1_rows_index.equal_range(t2_id3);  // 查找t1中是否有这个id3
            if (range.first != range.second) {               // 有的话就满足join谓语
                for (auto range_it = range.first; range_it != range.second; range_it++) {
                    auto find_it = group_map.find(std::make_pair(range_it->second->id2, t2_id2));  // 根据t1和t2的id2，查找对应的group
                    if (find_it == group_map.end()) {                                              // 新加入group
                        auto group = new Group();
                        group->t1_id1 = range_it->second->id1;
                        group->t2_id1 = t2_id1;
                        group_map.insert({std::make_pair(range_it->second->id2, t2_id2), group});
                    } else {  // 已有group，更新聚集函数
                        find_it->second->t1_id1 = std::max(find_it->second->t1_id1, range_it->second->id1);
                        find_it->second->t2_id1 = std::min(find_it->second->t2_id1, t2_id1);
                    }
                }
            }
        } else {
            break;
        }
    }

    std::vector<Group *> groups;
    for (auto it = group_map.begin(); it != group_map.end(); it++) {
        groups.push_back((*it).second);
    }
    return groups;
}

// sort 只比较 t1_id1。因为sort之前已经按照 group by 的那两个column有序了。
struct SortByComparator {
    bool operator()(const Group *group1, const Group *group2) const {
        return group1->t1_id1 < group2->t1_id1;
    }
};

int main() {
    std::string file1_name = "/home/web/ztedatabase/input1.csv";
    std::string file2_name = "/home/web/ztedatabase/input2.csv";

    // join
    // std::ifstream fin1(file1_name, std::ios::ate);
    // std::ifstream fin2(file2_name, std::ios::ate);
    // if (fin1.tellg() > fin2.tellg()) {  // 如果t1比较大，那么交换两个文件。交换后要注意聚集函数的不同，max，min。
    //     std::swap(file1_name, file2_name);
    // }
    auto groups = join_and_group(file1_name, file2_name);

    // sort
    std::stable_sort(groups.begin(), groups.end(), SortByComparator());

    // print result
    for (auto it = groups.begin(); it != groups.end(); it++) {
        std::cout << (*it)->t1_id1 << "," << (*it)->t2_id1 << std::endl;
    }

    return 0;
}
