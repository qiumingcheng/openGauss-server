/* Confinement and kernel anchor-fd admission for external file access. */
#ifndef OG_LAKE_EXTERNAL_PATH_H
#define OG_LAKE_EXTERNAL_PATH_H
#include "postgres.h"

struct LakeExternalOpenedFile {
    int fd;
    int64 file_bytes;
    char* display_name;
};

/* Validate and canonicalize a local directory root without symlinks or dots. */
extern char* LakeExternalValidateRoot(const char* root);

/* Reject remote object-storage and network URLs (explicit file:// allowed where stripped). */
extern bool LakeExternalIsRemoteUrl(const char* input);

/* Validate/infer canonical format string: "parquet", "csv", or "text". */
extern char* LakeExternalNormalizeFormat(const char* explicitFormat, const char* path);

/* Validate relative path components and expected filename suffix. */
extern bool LakeExternalValidRelativePath(const char* relative, const char* requiredFormat);

/* Open a read-only regular file confined strictly inside root via Linux openat2. */
extern LakeExternalOpenedFile LakeExternalOpenReadonly(const char* root, const char* relative,
    const char* requiredFormat);

/* Close an opened descriptor idempotently without throwing. */
extern void LakeExternalCloseOpenedFile(LakeExternalOpenedFile* opened) noexcept;

/* Split and authorize a standalone path string under safe_data_path / role privileges. */
extern void LakeExternalAuthorizePath(const char* rawPath, char** rootOut, char** relOut);

/* Authorize a standalone non-existent local target file for exclusive creation. */
/* If requested, the caller owns the parent directory descriptor on success. */
extern char* LakeExternalAuthorizeWritePath(const char* rawPath, int* parentOut = NULL);
#endif
