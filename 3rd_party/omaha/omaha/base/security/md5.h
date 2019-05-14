// Copyright 2007-2009 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// ========================================================================

#ifndef OMAHA_BASE_SECURITY_MD5_H_
#define OMAHA_BASE_SECURITY_MD5_H_

#include <stdint.h>
#include "hash-internal.h"

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

typedef HASH_CTX MD5_CTX;

void MD5_init(MD5_CTX* ctx);
void MD5_update(MD5_CTX* ctx, const void* data, unsigned int len);
const uint8_t* MD5_final(MD5_CTX* ctx);

// Convenience method. Returns digest address.
// NOTE: *digest needs to hold MD5_DIGEST_SIZE bytes.
const uint8_t* MD5_hash(const void* data, unsigned int len, uint8_t* digest);

#define MD5_DIGEST_SIZE 16

#ifdef __cplusplus
}
#endif // __cplusplus

#endif  // OMAHA_BASE_SECURITY_MD5_H_
