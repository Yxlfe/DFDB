//
// Created by apple on 2023/2/20.
//

#include "timer.h"

uint64_t get_now_micros() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (tv.tv_sec) * 1000000 + tv.tv_usec;
}