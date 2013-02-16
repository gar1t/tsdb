#include "test_core.h"

typedef u_int32_t word_t;

enum { BITS_PER_WORD = sizeof(word_t) * CHAR_BIT };

#define WORD_OFFSET(b) ((b) / BITS_PER_WORD)
#define BIT_OFFSET(b)  ((b) % BITS_PER_WORD)

void set_bit(word_t *words, int n) { 
    words[WORD_OFFSET(n)] |= (1 << BIT_OFFSET(n));
}

void clear_bit(word_t *words, int n) {
    words[WORD_OFFSET(n)] &= ~(1 << BIT_OFFSET(n)); 
}

int get_bit(word_t *words, int n) {
    word_t bit = words[WORD_OFFSET(n)] & (1 << BIT_OFFSET(n));
    return bit != 0; 
}

void print_title(char *title) {
    u_int32_t i, padding = 18 - strlen(title);
    printf("%s:", title);
    for (i = 0; i < padding; i++) {
        printf(" ");
    }
}

void scan_result(word_t *result, u_int32_t max_index, 
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

void print_index(u_int32_t *index) {
    printf(" %u", *index);
}

void print_result(word_t *result, u_int32_t max_index, char *title) {
    print_title(title);
    scan_result(result, max_index, print_index);
    printf("\n");
}

void compress_array(word_t *array, u_int32_t array_len,
                    char *compressed, u_int32_t *compressed_len) {
    u_int32_t new_len = array_len + 400;
    qlz_state_compress state_compress;
    memset(&state_compress, 0, sizeof(state_compress));
    compressed = (char*)malloc(new_len);
    *compressed_len = qlz_compress(array, compressed, array_len,
                                   &state_compress);
}

int main(int argc, char *argv[]) {
    u_int32_t i, j;
    u_int8_t word_size = sizeof(word_t);
    u_int32_t array_len = CHUNK_GROWTH / word_size;

    // three arrays, each representing a tag

    word_t tag1[array_len]; memset(tag1, 0, sizeof(tag1));
    word_t tag2[array_len]; memset(tag2, 0, sizeof(tag2));
    word_t tag3[array_len]; memset(tag3, 0, sizeof(tag3));

    // array for operation results

    word_t result[array_len];

    // to tag an index, we set the bit on the applicable array

    set_bit(tag1, 0);
    set_bit(tag3, 0);

    set_bit(tag2, 1);

    set_bit(tag2, 2);
    set_bit(tag3, 2);

    set_bit(tag1, 3);
    set_bit(tag2, 3);
    set_bit(tag3, 3);

    set_bit(tag3, 4);

    set_bit(tag1, 9999);

    u_int32_t max_index = 9999;
    u_int32_t array_max_used = max_index / word_size;

    printf("Size of tag array uncompressed: %lu\n", sizeof(tag1));

    char *compressed = 0;
    u_int32_t compressed_len;
    compress_array(tag1, sizeof(tag1), compressed, &compressed_len);
    printf("Size of tag array compressed: %u\n", compressed_len);
    free(compressed);

    printf("\n");

    // tag1
    //
    for (i = 0; i < array_max_used; i++) {
        result[i] = tag1[i];
    }
    print_result(result, max_index, "tag1");

    // tag1 and tag2
    //
    for (i = 0; i < array_max_used; i++) {
        result[i] = tag1[i] & tag2[i];
    }
    print_result(result, max_index, "tag1 and tag2");

    // tag2 and tag3
    //
    for (i = 0; i < array_max_used; i++) {
        result[i] = tag2[i] & tag3[i];
    }
    print_result(result, max_index, "tag2 and tag3");

    // tag1 or tag2
    //
    for (i = 0; i < array_max_used; i++) {
        result[i] = tag1[i] | tag2[i];
    }
    print_result(result, max_index, "tag1 or tag2");

    // tag2 or tag3
    //
    for (i = 0; i < array_max_used; i++) {
        result[i] = tag2[i] | tag3[i];
    }
    print_result(result, max_index, "tag2 or tag3");

    // tag1 and not tag2
    //
    for (i = 0; i < array_max_used; i++) {
        result[i] = tag1[i] & ~tag2[i];
    }
    print_result(result, max_index, "tag1 and not tag2");

    // calculate result and scan multiple times (demonstrate load)
    //
    for (i = 0; i < 100000; i++) {
        for (j = 0; j < array_max_used; j++) {
            result[j] = (tag1[j] & ~tag2[j]) | tag3[j];
        }
        scan_result(result, max_index, NULL);
    }

    return 0;
}
