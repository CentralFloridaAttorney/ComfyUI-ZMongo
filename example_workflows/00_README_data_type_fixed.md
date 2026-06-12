# LTXV ZMongo visual example workflows — data-type fixed

These workflow files are ComfyUI visual workflow JSONs for the example workflow menu. The previous package loaded, but several widgets were shifted because converted/linked widget placeholders were missing from `widgets_values`.

This package fixes the pre-filled widget values so common validation-sensitive fields keep the expected types:

- `ZMongoApiListDocsNode.limit`: integer >= 1
- `ZMongoApiListDocsNode.skip`: integer >= 0
- `ZMongoApiBrowseCollectionImagesNode.limit`: integer >= 1
- `ZMongoApiBrowseCollectionImagesNode.thumbnail_width`: integer >= 64
- `ZMongoApiBrowseCollectionImagesNode.thumbnail_height`: integer >= 64
- `GeminiImageTextNode.max_output_tokens`: integer >= 1
- `GeminiImageTextNode.temperature`: float <= 2.0
- `GeminiImageTextNode.max_image_side`: integer >= 256
- `GeminiImageTextNode.jpeg_quality`: integer
- `KSampler.cfg`: float
- `KSampler.sampler_name`: string sampler name
- `KSampler.scheduler`: string scheduler name

API key fields are blank by design. Paste your key into the API session node before running API-backed workflows.

Generated: 2026-06-12T13:44:55.501280Z

Audit result: PASS
