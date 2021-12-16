/*
 * fingerprint_cache.h
 */

#ifndef FINGERPRINT_CACHE_H_
#define FINGERPRINT_CACHE_H_

void init_fingerprint_cache();
int64_t fingerprint_cache_lookup(fingerprint *fp);
void fingerprint_cache_prefetch(int64_t id);

#endif /* FINGERPRINT_CACHE_H_ */