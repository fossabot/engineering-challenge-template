#!/usr/bin/env python3
"""
Automated Mermaid diagram renderer for architecture.md.
Extracts all ```mermaid``` blocks and renders them as SVG to docs/architecture/.
"""

import re
import subprocess
import sys
from pathlib import Path

ARCHITECTURE_MD = Path("docs/architecture.md")
OUTPUT_DIR = Path("docs/architecture")
MMMD_TEMP_DIR = Path("/tmp/arch_diagrams")

def extract_diagrams(md_path: Path) -> list[str]:
    """Extract all mermaid code blocks from architecture.md."""
    content = md_path.read_text()
    # Pattern: ```mermaid\n...diagram...\n```
    pattern = r'```mermaid\n(.*?)\n```'
    matches = re.findall(pattern, content, re.DOTALL)
    return [m.strip() for m in matches]

def render_diagram(mmd_content: str, output_path: Path) -> bool:
    """Render a single mermaid diagram to SVG using mmdc."""
    MMMD_TEMP_DIR.mkdir(parents=True, exist_ok=True)
    temp_mmd = MMMD_TEMP_DIR / f"{output_path.stem}.mmd"
    temp_mmd.write_text(mmd_content)
    
    # Use puppeteer from cache
    chrome_path = Path.home() / ".cache/puppeteer/chrome-headless-shell/mac_arm-156.0.8078.47/chrome-headless-shell-mac-arm64/chrome-headless-shell"
    env = {"PUPPETEER_EXECUTABLE_PATH": str(chrome_path)} if chrome_path.exists() else {}
    
    try:
        result = subprocess.run(
            ["mmdc", "-i", str(temp_mmd), "-o", str(output_path), "-b", "transparent"],
            capture_output=True,
            text=True,
            timeout=60,
            env={**subprocess.os.environ, **env}
        )
        if result.returncode != 0:
            print(f"  ❌ Failed: {result.stderr[:200]}")
            return False
        return True
    except subprocess.TimeoutExpired:
        print(f"  ❌ Timeout")
        return False
    except FileNotFoundError:
        print(f"  ❌ mmdc not found. Install with: npm install -g @mermaid-js/mermaid-cli")
        return False

def main():
    if not ARCHITECTURE_MD.exists():
        print(f"❌ {ARCHITECTURE_MD} not found")
        sys.exit(1)
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    diagrams = extract_diagrams(ARCHITECTURE_MD)
    print(f"Found {len(diagrams)} mermaid diagrams in {ARCHITECTURE_MD}")
    
    success = 0
    for i, diagram in enumerate(diagrams, 1):
        output_file = OUTPUT_DIR / f"architecture_diagram_{i}.svg"
        print(f"  Rendering diagram {i}/{len(diagrams)} → {output_file.name}...", end=" ")
        if render_diagram(diagram, output_file):
            print("✅")
            success += 1
        else:
            print("❌")
    
    print(f"\n✅ {success}/{len(diagrams)} diagrams rendered successfully")
    
    if success != len(diagrams):
        sys.exit(1)

if __name__ == "__main__":
    main()