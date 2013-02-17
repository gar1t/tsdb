#include <stdlib.h>
#include <limits.h>

enum { BITS_PER_WORD = sizeof(u_int32_t) * CHAR_BIT };

#define WORD_OFFSET(b) ((b) / BITS_PER_WORD)
#define BIT_OFFSET(b)  ((b) % BITS_PER_WORD)

void set_bit(u_int32_t *words, int n);

void clear_bit(u_int32_t *words, int n);

int get_bit(u_int32_t *words, int n);

void scan_result(u_int32_t *result,
                 u_int32_t max_index, 
                 void(*handler)(u_int32_t *index));
