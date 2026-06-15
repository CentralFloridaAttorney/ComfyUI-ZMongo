# LTXV ZMongo Modular Visual Workflows

These files are ComfyUI visual workflow JSON files, not API prompt JSON files. They include visible canvas nodes, links, groups, positions, widget values, and matching JPG thumbnails for the example_workflows menu.

## Run order

1. 00_select_image_browser.workflow.json
2. 01_save_ltxv_system_prompt.workflow.json, only when updating the system prompt
3. 03_generate_ltxv_prompt_from_selected_image.workflow.json
4. 04_select_load_ltxv_video_prompt.workflow.json
5. 05_render_ltxv_video_simple.workflow.json or 06_render_ltxv_video_advanced.workflow.json

## Shared paths

- prompt.system.ltxv_i2v_121f_16fps_720p
- prompt.ltxv_i2v_121f_16fps_720p.positive
- image_data
- mask_data

## Important

Paste your API key into the API Key Session node before running hosted ZMongo/Gemini sections.
The 99_full_reference file is included only as a full reference graph and is not intended to be queued all at once.
