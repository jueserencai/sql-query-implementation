#include<iostream>
#include<fstream>
#include<algorithm>
#include<random>

int main() {

   /* std::ofstream fout("test.txt");
    for (int i = 0; i < 50; i++) {
        fout << i << " " << i + 1 << std::endl;
    }
    fout.close();

    std::ofstream f("test.txt", std::ios::in | std::ios::out);
    f.seekp(10);

    f.write("test test", 9);*/


    std::ofstream fout1("input1.csv");
    std::ofstream fout2("input2.csv");

    std::random_device rd; // obtain a random number from hardware
    std::mt19937 eng(rd()); // seed the generator
    std::uniform_int_distribution<> rand_t1_id2(1, 1000); // define the range
    std::uniform_int_distribution<> rand_t1_id3(1, 1000); // define the range
    std::uniform_int_distribution<> rand_t2_id2(1, 1000); // define the range
    std::uniform_int_distribution<> rand_t2_id3(1, 1000); // define the range

    std::vector<std::vector<int>> t1_rows;
    for (int i = 1; i <= 3000; i++) {
        std::vector<int> row;
        row.push_back(i);
        row.push_back(rand_t1_id2(eng));
        row.push_back(rand_t1_id3(eng));
        t1_rows.push_back(row);
    }
    std::shuffle(t1_rows.begin(), t1_rows.end(), eng);
    for (int i = 0; i < t1_rows.size(); i++) {
        auto& row = t1_rows[i];
        fout1 << row[0] << "," << row[1] << "," << row[2] << std::endl;
    }

    std::vector<std::vector<int>> t2_rows;
    for (int i = 1; i <= 3000; i++) {
        std::vector<int> row;
        row.push_back(i);
        row.push_back(rand_t2_id2(eng));
        row.push_back(rand_t2_id3(eng));
        t2_rows.push_back(row);
    }
    std::shuffle(t2_rows.begin(), t2_rows.end(), eng);
    for (int i = 0; i < t2_rows.size(); i++) {
        auto& row = t2_rows[i];
        fout2 << row[0] << "," << row[1] << "," << row[2] << std::endl;
    }
}