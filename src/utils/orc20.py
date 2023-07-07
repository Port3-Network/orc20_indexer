import re
import simplejson as json


def is_standardize_orc20(content_str):
    try:
        # if the json str with comma at the end, remove it
        cleaned_content = re.sub(r',\s*\}', '}', content_str)
        content = json.loads(cleaned_content)

        if not isinstance(content, dict):
            return None

        if "p" not in content or \
                content["p"].lower() not in ["orc20", "orc-20"]:
            return None

        if "op" not in content:
            return None

        return content

    except Exception:
        return None


def is_orc20(content_str):

    # check if it is standard json
    content = is_standardize_orc20(content_str)
    if content is not None:
        return content

    # check if it is multi json
    matches = re.findall(r'\{.*?\}', content_str, re.DOTALL)
    if len(matches) < 2:
        # there are non-standard strings outside the JSON brackets, invalid
        return None
    for match in matches:
        content = is_standardize_orc20(match)
        if content is not None:
            return content
