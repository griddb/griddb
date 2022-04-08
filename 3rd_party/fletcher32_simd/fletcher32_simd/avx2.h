#include <stdint.h>
#include <stddef.h>

/* AVX2 based implementation of Fletcher32
 * @param  data    pointer to input data, ideally aligned
 * @param  len   length of input data in terms of *uint16_t*
 * @param  a       sum counter 1 in fletcher algorithm, between 0 and 65535
 * @param  b       sum counter 2 in fletcher algorithm, between 0 and 65535
 */
uint32_t fletcher32_avx2 (uint16_t* data, size_t len, uint32_t& a, uint32_t& b);
