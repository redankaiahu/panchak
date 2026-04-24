import docx2txt
import os

try:
    text = docx2txt.process("openinterest.docx")
    with open("openinterest.txt", "w", encoding="utf-8") as f:
        f.write(text)
    print("Successfully converted openinterest.docx to openinterest.txt")
except Exception as e:
    print(f"Error: {e}")
