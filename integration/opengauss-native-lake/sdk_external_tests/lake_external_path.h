/* Standalone candidate for the caller side of the SDK6 descriptor contract. */
#ifndef OG_LAKE_EXTERNAL_PATH_H
#define OG_LAKE_EXTERNAL_PATH_H

#ifdef __cplusplus
extern "C" {
#endif

/* Only call after authorizing root configuration and the calling SQL relation.
 * On success the caller owns one O_RDONLY, close-on-exec regular file fd. It
 * is the sole input to the SDK; the caller must not reopen the string path.
 * Any nonzero error leaves no descriptors behind; errno is preserved. */
int og_lake_open_external_parquet(const char* root,const char* relative);

#ifdef __cplusplus
}
#endif
#endif