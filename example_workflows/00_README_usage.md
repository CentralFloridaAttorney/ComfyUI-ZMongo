# ZMongo LTXV Modular Workflows

These files split the original oversized workflow into independently runnable ComfyUI API/prompt JSON files.

The uploaded source workflow was an API-style prompt JSON, not a visual UI workflow with node positions and group boxes. For reliability, the modular version is separated into smaller runnable workflow JSON files instead of one giant graph.

## Shared defaults

- Image collection: `succubi`
- Image field path: `image_data`
- Mask field path: `mask_data`
- Prompt collection: `prompts`
- LTXV positive prompt path: `prompt.ltxv_i2v_121f_16fps_720p.positive`
- LTXV system prompt path: `prompt.system.ltxv_i2v_121f_16fps_720p`
- Target video length: `121` frames
- Target frame rate: `16` FPS
- Target image/video prep size: `1280x720`

The API key field was intentionally cleared. Paste your API key into node `260`, titled:

`00 API Key Session - paste key here`

The session switch node `261` controls hosted API vs local file store.

## Suggested use order

### 1. Select or preview source image

Open:

`00_select_image_browser.workflow.json`

Use this to list image records, browse thumbnails, and preview the selected image.

Important nodes:

- `270` — image collection name
- `244` — image field path
- `245` — list docs
- `279` — selected image index
- `291` — display selected image
- `290` — preview selected image

Run this workflow by itself when you only want to change or confirm the source image.

### 2. Save or update the LTXV system prompt

Open:

`01_save_ltxv_system_prompt.workflow.json`

This saves the Gemini prompt-constructor system prompt into:

`prompt.system.ltxv_i2v_121f_16fps_720p`

Important nodes:

- `325` — editable system prompt text
- `322` — system prompt field path
- `286` — save value node
- `287` — save-result preview

Run this only when changing the system instruction.

### 3. Manually save a positive video prompt

Open:

`02_manual_save_ltxv_positive_prompt.workflow.json`

Use this when you already have a finished LTXV prompt and want to save it without calling Gemini.

Important nodes:

- `900` — manual positive prompt text
- `295` — save generated/manual positive prompt
- `294` — save-result preview

The prompt saves to:

`prompt.ltxv_i2v_121f_16fps_720p.positive`

### 4. Generate a positive LTXV prompt from the selected image

Open:

`03_generate_ltxv_prompt_from_selected_image.workflow.json`

Use this when you want Gemini to look at the selected image and the rough concept, then produce a clean LTXV image-to-video prompt and save it.

Important nodes:

- `289` — rough user video concept
- `279` — selected image index
- `291` — selected image
- `325` — system prompt
- `301` — Gemini image + text prompt generator
- `295` — save generated prompt
- `293` — Gemini response preview
- `294` — save-result preview

This workflow does not render video. It only generates and saves the video prompt.

### 5. Select/load an existing video prompt

Open:

`04_select_load_ltxv_video_prompt.workflow.json`

Use this to select a prompt document and preview the saved positive prompt before rendering.

Important nodes:

- `262` — prompt collection name
- `267` — list prompt docs
- `283` — selected prompt document index
- `296` — get saved LTXV positive prompt
- `297` — loaded prompt preview

### 6. Render simple LTXV video

Open:

`05_render_ltxv_video_simple.workflow.json`

This is the smaller LTXV render path using:

- `ltx-video-2b-v0.9.5.safetensors`
- `t5xxl_fp16.safetensors`
- `LTXVImgToVideo`
- 121 frames
- 16 FPS

Important nodes:

- `279` — selected source image index
- `296` / `297` — loaded positive prompt
- `77` — LTXV image-to-video node
- `72` — sampler
- `81` — video save node

Note: node `77` uses `1280x736` because some LTXV/video latent paths require dimensions divisible by model-friendly latent sizes. The source image selection/prep is still `1280x720`.

### 7. Render advanced LTXV video/audio path

Open:

`06_render_ltxv_video_advanced.workflow.json`

This is the larger advanced path using:

- `ltx-2-19b-distilled.safetensors`
- `gemma_3_12B_it_fp4_mixed.safetensors`
- latent upscaler
- audio latent path
- 121 frames
- 16 FPS

Important nodes:

- `279` — selected source image index
- `296` / `297` — loaded positive prompt
- `310` — resize selected image to 1280x720
- `316:62` — frame length
- `321` — frame rate
- `316:3` — loaded prompt text encode
- `315` — save final video

Use this only after the prompt and selected image are correct.

### 8. Generate and save a new reference image

Open:

`07_generate_and_save_reference_image.workflow.json`

Use this to generate a new still image from the selected/loaded prompt and save it back to ZMongo.

Important nodes:

- `296` / `297` — loaded prompt
- `236` — SD3.5 checkpoint
- `280` — image sampler
- `240` — save generated image
- `281` — save-result preview

### 9. Save and browse mask from selected image

Open:

`08_save_and_browse_mask_from_selected_image.workflow.json`

Use this only when you need a mask generated from the selected image and saved to the mask field path.

Important nodes:

- `291` — selected image
- `274` — image to mask
- `248` — mask to image
- `247` — save mask image
- `250` — browse saved masks
- `276` / `277` — previews

## Practical workflow pattern

1. Run `00_select_image_browser.workflow.json`
2. Run `01_save_ltxv_system_prompt.workflow.json` only if the system prompt changed
3. Run `03_generate_ltxv_prompt_from_selected_image.workflow.json`
4. Run `04_select_load_ltxv_video_prompt.workflow.json`
5. Run either:
   - `05_render_ltxv_video_simple.workflow.json`, or
   - `06_render_ltxv_video_advanced.workflow.json`

## Why this is more functional

The old workflow mixed these actions into one graph:

- source-image selection
- system-prompt saving
- Gemini prompt construction
- prompt saving
- prompt loading
- image generation
- mask generation
- simple LTXV rendering
- advanced LTXV rendering

That made ComfyUI try to evaluate unrelated nodes. These modular files prevent unnecessary model loading and let each workflow do one job.
