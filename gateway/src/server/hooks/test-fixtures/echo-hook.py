import json
import os
import sys

invocation = json.load(sys.stdin)
json.dump(
    {
        "action": "continue",
        "metadata": {
            "runtime": "python",
            "invocationId": invocation["invocationId"],
            "body": invocation["message"]["body"],
            "allowedValue": os.environ.get("HOOK_ALLOWED"),
        },
    },
    sys.stdout,
    separators=(",", ":"),
)
