# Dropbox Cleaner Slides

This directory contains the editable PowerPoint deck and the JavaScript source used to generate it.

## Files

- `build_deck.js` - PptxGenJS source
- `DropboxCleaner_Open_Source_Overview.pptx` - generated deck
- `rendered/` - exported PNG slide previews
- `export_powerpoint_pngs.py` - Windows PowerPoint renderer

## Rebuild

```powershell
cd docs/slides
npm install
npm run build
py -3.11 export_powerpoint_pngs.py .\DropboxCleaner_Open_Source_Overview.pptx --output-dir .\rendered
py -3.11 C:\Users\Asus\.codex\skills\slides\scripts\create_montage.py --input_dir .\rendered --output_file .\rendered\montage.png --label_mode filename --num_col 3
```
