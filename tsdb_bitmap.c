#include "tsdb_bitmap.h"

void set_bit(u_int32_t *words, int n) { 
    words[WORD_OFFSET(n)] |= (1 << BIT_OFFSET(n));
}

void clear_bit(u_int32_t *words, int n) {
    words[WORD_OFFSET(n)] &= ~(1 << BIT_OFFSET(n)); 
}

int get_bit(u_int32_t *words, int n) {
    u_int32_t bit = words[WORD_OFFSET(n)] & (1 << BIT_OFFSET(n));
    return bit != 0; 
}

void scan_result(u_int32_t *result, u_int32_t max_index, 
                 void(*handler)(u_int32_t *index)) {
    u_int32_t i, j, index;
    u_int32_t max_word = max_index / BITS_PER_WORD;

    for (i = 0; i <= max_word; i++) {
        if (result[i] == 0) {
            continue;
        }
        for (j = 0; j < BITS_PER_WORD; j++) {
            index = i * BITS_PER_WORD + j;
            if (index > max_index) {
                break;
            }
            if (get_bit(result, index) && handler) {
                handler(&index);
            }
        }
    }
}
