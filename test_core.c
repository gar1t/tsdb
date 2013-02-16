#include "test_core.h"

int file_exists(char *filename) {
    FILE *file;
    if ((file = fopen(filename, "r"))) {
        fclose(file);
        return 1;
    } else {
        return 0;
    }
}

