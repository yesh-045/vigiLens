# Reranker Service (Simple Setup)

Modal-deployed vLLM service for scoring videos via:

POST `/v1/score`

---

## 1. Requirements

* Modal account
* Python 3.11+
* Repo cloned
* (Optional) Hugging Face token

---

## 2. Install Modal

```bash
python -m pip install -U modal
modal setup
modal profile current
```

---

## 3. Create Secrets

```bash
modal secret create vllm-api-key VLLM_API_KEY="your-key"
modal secret create huggingface-secret HF_TOKEN="hf_xxx"
```

---

## 4. Deploy

```bash
make deploy-reranker
# or
modal deploy vigilens/reranker/service.py
```

Save the generated URL → `SCREENER_BASE_URL`

---

## 5. Test API

```python
import base64, json, os, urllib.request

BASE_URL = os.environ["SCREENER_BASE_URL"]
API_KEY = os.environ["SCREENER_API_KEY"]

with open("sample.mp4", "rb") as f:
    b64 = base64.b64encode(f.read()).decode()

payload = {
    "model": "Qwen/Qwen3-VL-Reranker-2B",
    "queries": ["person falling", "fire"],
    "documents": {
        "content": [
            {"type": "video_url", "video_url": {"url": f"data:video/mp4;base64,{b64}"}},
            {"type": "video_url", "video_url": {"url": f"data:video/mp4;base64,{b64}"}},
        ]
    }
}

req = urllib.request.Request(
    f"{BASE_URL}/v1/score",
    data=json.dumps(payload).encode(),
    headers={
        "Content-Type": "application/json",
        "Authorization": f"Bearer {API_KEY}",
    },
)

with urllib.request.urlopen(req) as r:
    print(r.status)
    print(r.read().decode()[:500])
```

Expected:

* 200 response
* JSON with scores

---

## 6. Configure App

```bash
SCREENER_BASE_URL="https://your-url"
SCREENER_API_KEY="same-as-secret"
SCREENER_MODEL="Qwen/Qwen3-VL-Reranker-2B"
```

Run:

```bash
make up
```

---

## 7. Operations

```bash
make deploy-reranker-logs
make deploy-reranker-shell
make deploy-reranker-stop
make deploy-reranker
```

---

## Common Issues

**401 Unauthorized**
Wrong API key

**Slow startup**
Increase timeouts or set `min_containers: 1`

**Model download fails**
Check `HF_TOKEN`

**GPU issues**
Change GPU or reduce load
