import docx2txt
import os

out_dir = "magiclines_content"
if not os.path.exists(out_dir):
    os.makedirs(out_dir)

try:
    # docx2txt.process returns text AND can extract images to a directory
    text = docx2txt.process("magiclines.docx", out_dir)
    print(f"Text length extracted: {len(text)}")
    with open("magiclines_full.txt", "w", encoding="utf-8") as f:
        f.write(text)
    
    files = os.listdir(out_dir)
    print(f"Extracted files in {out_dir}: {files}")
except Exception as e:
    print(f"Error: {e}")
