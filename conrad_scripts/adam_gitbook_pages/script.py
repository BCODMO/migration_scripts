import requests
from bs4 import BeautifulSoup

base_url = "https://demo.bco-dmo.org"

loop_paths = set(["/how-to"])
new_paths = set()
used_paths = set()
found_html = {}
while len(loop_paths) != 0:
    for path in loop_paths:
        r = requests.get(f"{base_url}/{path}")
        html = r.text
        found_html[path] = html

        # Now find anything linked to here
        soup = BeautifulSoup(html, "html.parser")
        anchors = soup.find_all("a", href=True)
        for a in anchors:
            href = a["href"]
            if "#" in href:
                href = href[: href.index("#")]
            if (
                href.startswith("/how-to")  # local path
                and len(href) > 1  # not just going to base
                and href not in used_paths  # haven't seen before
                and href not in loop_paths  # haven't seen before
                and href not in loop_paths  # haven't seen before
            ):
                new_paths.add(href)

    used_paths.update(loop_paths)
    loop_paths = new_paths
    new_paths = set()
    print(f"Used {len(used_paths)} paths, looping through {len(loop_paths)} more now.")
    if len(loop_paths) != 12:
        # break
        pass

print("All paths found:", list(found_html.keys()))
exit()

prompts = []
for page in found_html.keys():
    html = found_html[page]
    soup = BeautifulSoup(html, "html.parser")
    s = ""
    title = ""
    for para in soup.find_all(["span", "strong"], text=True):
        if isinstance(para.contents[0], str):
            # print(para.get_text())
            text = para.get_text()
            if (not text.isupper() or len(text) <= 1) and not (
                para.parent.parent.has_attr("class")
                and "r-1rasi3h" in para.parent.parent["class"]
            ):
                if para.parent.parent.has_attr(
                    "data-rnwrdesktop-gg6oyi-1x35g6-37tt59-b88u0q"
                ):
                    if s:
                        prompts.append(
                            (
                                title,
                                s,
                            )
                        )
                        print(f"TITLE: {title}")
                        print(s)
                        print("-----------------")
                        s = ""
                        title = ""
                    title = text
                else:
                    s += text
questions = []
statements = []
for (title, text) in prompts:
    if title.endswith("?"):
        questions.append(
            (
                title,
                text,
            )
        )
    else:
        statements.append(
            (
                title,
                text,
            )
        )
print(f"Num questions: {len(questions)}, Num statements: {len(statements)}")

import openai
import os
import re

openai.organization = "org-FHUdj2qCxSdtXsMNGUVm3s0z"
openai.api_key = os.getenv("OPENAI_API_KEY")


def get_completion(prompt, model="gpt-3.5-turbo"):
    messages = [{"role": "user", "content": prompt}]
    response = openai.ChatCompletion.create(
        model=model,
        messages=messages,
        temperature=0,
    )

    return response.choices[0].message["content"]


for (title, text) in statements:
    prompt = f"""You are creating training data for a fine tuned LLM that will eventually answer questions from users. The users are scientists that are using the BCO-DMO website, a data repository for chemical and biological oceanography data. I will give you a title and a paragraph and you will convert that text into a question and an answer. Respond in the following format exactly:
    QUESTION=...
    ANSWER=...

The title is:
    {title}.
The text is:
    {text}
"""
    print(f"Getting prompt for {title}")
    t = get_completion(prompt)

    print(t)
    result = re.search(r"QUESTION=([\S\n ]*)ANSWER=([\S\n ]*)", t)
    prompt, response = result.groups()
    questions.append(
        (
            prompt,
            response,
        )
    )


import csv

with open("finetuning.csv", "w", newline="") as csvfile:
    writer = csv.writer(
        csvfile,
        delimiter=",",
    )
    writer.writerow(["prompt", "completion"])
    for (prompt, text) in questions:
        writer.writerow([prompt, text])
