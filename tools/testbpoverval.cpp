#include <iostream>
#include "wttool.h"
#include "wtbptree.hpp"

using namespace wt;
using namespace std;

int main() {
    
    BPTree<int, int> tree;
    tree.init("bptree.data", 2);
    
    for (int i = 0; i < 100; ++i) {
        for (int j = 0; j < 100; ++j) {
            tree.insert(i, i * 1000 + j);
            //cout << "\n\n\n" << i << " " << j << "\n";
            //tree.print_structure();
        }
    }
    
    vector<int> res;
    cout << "NUM: " << tree.search(59, &res) << endl;
    for (int i = 0; i < res.size(); ++i) {
       cout << res[i] << " " << endl;
    }
    
    cout << endl;
    return 0;
}