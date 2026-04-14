# ComfyUI-ZMongo Session Standardized Rewrite

Changes:
- Session refresh is forced before every manager request.
- Structured outputs are standardized to `ZMONGO_JSON`.
- Most nodes now return `(result_json, data_json, text_output, success)`.
- Added helper JSON nodes:
  - ZMongo JSON Extract Items
  - ZMongo JSON Select Item
  - ZMongo JSON Select Path
  - ZMongo JSON List Paths
