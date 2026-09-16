#!/usr/bin/env python3
"""Stamp port dependencies from verified pushed sibling revisions."""
import json, re, subprocess
from pathlib import Path
root=Path(__file__).resolve().parents[1]
def pushed(repo):
    sha=subprocess.check_output(['git','-C',str(repo),'rev-parse','HEAD'],text=True).strip()
    refs=subprocess.check_output(['git','-C',str(repo),'branch','-r','--contains',sha],text=True)
    if 'origin/' not in refs: raise SystemExit(f'{repo}: HEAD is not pushed')
    return sha
red=pushed(root.parent/'red');blue=pushed(root.parent/'blue')
for file in [root/'package.json',root/'red/package.json']:
    doc=json.loads(file.read_text())
    for key in ['dependencies','overrides']:doc[key]['red']='github:getcolors/red#'+red
    file.write_text(json.dumps(doc,indent=2)+'\n')
p=root/'blue/pyproject.toml';s=p.read_text();s=re.sub(r'(blue = \{ git = "https://github.com/getcolors/blue.git", rev = ")[^"]+',lambda m:m[1]+blue,s);s=re.sub(r'(blue @ git\+https://github.com/getcolors/blue.git@)[a-f0-9]+',lambda m:m[1]+blue,s);p.write_text(s)
p=root/'skills/package-redis-operator-red/red';s=p.read_text();s=re.sub(r'("red": ")github:getcolors/red#[a-f0-9]+',lambda m:m[1]+'github:getcolors/red#'+red,s);p.write_text(s)
