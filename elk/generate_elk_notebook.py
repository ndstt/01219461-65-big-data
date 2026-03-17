from __future__ import annotations

import json
from pathlib import Path

import nbformat as nbf


ROOT = Path("/home/hadoop/homework/elk")
NOTEBOOK_PATH = ROOT / "elk_homework.ipynb"


def read_text(path: Path) -> str:
    return path.read_text().rstrip()


def main() -> None:
    nb = nbf.v4.new_notebook()
    nb["metadata"]["kernelspec"] = {
        "display_name": "Python 3",
        "language": "python",
        "name": "python3",
    }
    nb["metadata"]["language_info"] = {"name": "python", "version": "3.10"}

    logstash_conf = read_text(ROOT / "logstash_python_logger.conf")
    logger_code = read_text(ROOT / "python_log_generator.py")
    health_script = read_text(ROOT / "check_elastic_health.sh")

    nb.cells = [
        nbf.v4.new_markdown_cell(
            "# ELK Homework\n"
            "\n"
            "This notebook captures the local Elasticsearch, Kibana, and Logstash setup for the homework.\n"
            "It includes the Logstash pipeline, the Python async logger, a shell health check, and the Kibana URLs to submit."
        ),
        nbf.v4.new_markdown_cell(
            "## Submission Links\n"
            "\n"
            "- Kibana console for the `shakespeare` dataset: `http://10.3.134.62:5601/app/dev_tools#/console?_g=()`\n"
            "- Kibana console for the top-10 word aggregation on `python-logstash-homework-*`: `http://10.3.134.62:5601/app/dev_tools#/console?_g=()`\n"
            "\n"
            "Run the Dev Tools queries below in Kibana to show the homework datasets."
        ),
        nbf.v4.new_code_cell(
            "from pathlib import Path\n"
            "ROOT = Path('/home/hadoop/homework/elk')\n"
            "ROOT"
        ),
        nbf.v4.new_markdown_cell("## Logstash Pipeline\n\n```conf\n" + logstash_conf + "\n```"),
        nbf.v4.new_markdown_cell("## Python Async Logger\n\n```python\n" + logger_code + "\n```"),
        nbf.v4.new_markdown_cell("## Elasticsearch Health Script\n\n```bash\n" + health_script + "\n```"),
        nbf.v4.new_markdown_cell("## Cluster Health Output"),
        nbf.v4.new_code_cell(
            "!bash /home/hadoop/homework/elk/check_elastic_health.sh"
        ),
        nbf.v4.new_markdown_cell("## Homework Queries For Kibana Dev Tools"),
        nbf.v4.new_code_cell(
            "import json\n\n"
            "shakespeare_query = {\n"
            "    'query': {'match': {'play_name': 'Hamlet'}},\n"
            "    'size': 3,\n"
            "}\n"
            "top_words_query = {\n"
            "    'size': 0,\n"
            "    'aggs': {\n"
            "        'top_words': {\n"
            "            'terms': {\n"
            "                'field': 'top_word.keyword',\n"
            "                'size': 10,\n"
            "                'order': {'_count': 'desc'},\n"
            "            }\n"
            "        }\n"
            "    },\n"
            "}\n"
            "print('GET shakespeare/_search')\n"
            "print(json.dumps(shakespeare_query, indent=2))\n"
            "print()\n"
            "print('GET python-logstash-homework-*/_search')\n"
            "print(json.dumps(top_words_query, indent=2))"
        ),
        nbf.v4.new_markdown_cell("## Current Counts From Elasticsearch"),
        nbf.v4.new_code_cell(
            "import json\n"
            "import subprocess\n"
            "\n"
            "def curl_json(url: str, method: str = 'GET', payload: dict | None = None):\n"
            "    cmd = ['curl', '-sS', '-X', method, url]\n"
            "    if payload is not None:\n"
            "        cmd.extend(['-H', 'Content-Type: application/json', '-d', json.dumps(payload)])\n"
            "    out = subprocess.check_output(cmd, text=True)\n"
            "    return json.loads(out)\n"
            "\n"
            "cluster_health = curl_json('http://127.0.0.1:9200/_cluster/health')\n"
            "shakespeare_count = curl_json('http://127.0.0.1:9200/shakespeare/_count')\n"
            "top_words = curl_json(\n"
            "    'http://127.0.0.1:9200/python-logstash-homework-*/_search',\n"
            "    method='GET',\n"
            "    payload={\n"
            "        'size': 0,\n"
            "        'aggs': {\n"
            "            'top_words': {\n"
            "                'terms': {\n"
            "                    'field': 'top_word.keyword',\n"
            "                    'size': 10,\n"
            "                }\n"
            "            }\n"
            "        },\n"
            "    },\n"
            ")\n"
            "cluster_health, shakespeare_count, top_words['aggregations']['top_words']['buckets']"
        ),
    ]

    NOTEBOOK_PATH.write_text(json.dumps(nb, indent=1))


if __name__ == "__main__":
    main()
