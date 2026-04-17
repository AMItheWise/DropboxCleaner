from __future__ import annotations

import argparse
import re
from pathlib import Path

import win32com.client


def export_slides(pptx_path: Path, output_dir: Path, width: int, height: int) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    powerpoint = win32com.client.Dispatch("PowerPoint.Application")
    powerpoint.Visible = 1
    presentation = None
    try:
        presentation = powerpoint.Presentations.Open(str(pptx_path.resolve()), WithWindow=False, ReadOnly=True)
        presentation.Export(str(output_dir.resolve()), "PNG", width, height)
    finally:
        if presentation is not None:
            presentation.Close()
        powerpoint.Quit()

    for exported in output_dir.glob("*.PNG"):
        match = re.search(r"(\d+)$", exported.stem)
        if match is None:
            continue
        slide_number = int(match.group(1))
        target = output_dir / f"slide-{slide_number}.png"
        if target.exists():
            target.unlink()
        exported.rename(target)


def main() -> None:
    parser = argparse.ArgumentParser(description="Export PowerPoint slides to PNG images using Microsoft PowerPoint.")
    parser.add_argument("pptx_path", type=Path, help="Path to the PowerPoint deck.")
    parser.add_argument("--output-dir", type=Path, default=Path("rendered"), help="Directory to write PNG slides into.")
    parser.add_argument("--width", type=int, default=1600, help="Export width in pixels.")
    parser.add_argument("--height", type=int, default=900, help="Export height in pixels.")
    args = parser.parse_args()
    export_slides(args.pptx_path, args.output_dir, args.width, args.height)


if __name__ == "__main__":
    main()
