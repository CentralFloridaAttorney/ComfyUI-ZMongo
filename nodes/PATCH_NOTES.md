# ComfyUI-ZMongo route fix package

This package updates the uploaded node files to use the deployed production route shape confirmed by smoke testing:

- `GET /comfy-zmongo/health`
- `GET /comfy-zmongo/api/collections`
- `GET /comfy-zmongo/api/docs/<collection>`
- `POST /comfy-zmongo/api/doc/create`
- `POST /comfy-zmongo/api/doc/update`
- `POST /comfy-zmongo/api/doc/delete`
- `POST /comfy-zmongo/api/save-value`

Key fixes:

1. API keys are now sent through explicit API-key headers (`X-API-Key`, `X-ZAI-API-Key`, `X-ZMongo-API-Key`, `ZAI_API_KEY`) instead of being forced into `Authorization: Bearer`, because `/api/auth/verify` treats Bearer as JWT validation.
2. `ZMongoApiSession._join_path()` normalizes stale route variants and avoids duplicate `/comfy-zmongo/comfy-zmongo/...` paths.
3. The generic `_session_api_request()` wrapper now delegates into the prefix-aware route helper used by the newer nodes.
4. Document/MOTD defaults now point at the deployed `/comfy-zmongo` family instead of stale `/documents` or bearer-only auth.

Install by copying these files over the matching files in `custom_nodes/ComfyUI-ZMongo/`, then restart ComfyUI.
