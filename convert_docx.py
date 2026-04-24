import docx2txt
import sys

try:
    text = docx2txt.process("magiclines.docx")
    with open("magiclines.txt", "w", encoding="utf-8") as f:
        f.write(text)
    print("Successfully converted magiclines.docx to magiclines.txt")
except Exception as e:
    print(f"Error: {e}")
