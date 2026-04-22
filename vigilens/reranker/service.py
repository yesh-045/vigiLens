from modal import App, Image, Volume, concurrent, web_server, Secret
from yaml import safe_load
import os

local_dir = os.path.dirname(__file__)
config_path = os.path.join(local_dir, "config.yaml")
with open(config_path, "r") as f:
    config = safe_load(f)

app = App(config["service"]["name"])

# ------------------------------------------------------------
# Constants
# ------------------------------------------------------------

N_GPU = config["service"]["n_gpu"]
MINUTES = 60  # seconds
VLLM_PORT = config["service"]["port"]
MODEL_NAME = config["model"]["name"]
MODEL_REVISION = config["model"]["revision"]
FAST_BOOT = config["service"]["fast_boot"]
MAX_MODEL_LEN = config["model"]["max_model_len"]


# ------------------------------------------------------------
# Image
# ------------------------------------------------------------

vllm_image = (
    Image.from_registry(config["service"]["image"], add_python="3.12")
    .entrypoint([])
    .uv_pip_install(
        *config["service"]["pip_install"],
    )
    .env(config["service"]["env"])
    .add_local_file(
        os.path.join(local_dir, "qwen3_reranker.jinja"),
        remote_path="/root/qwen3_reranker.jinja",
        copy=True,
    )
    .add_local_file(
        os.path.join(local_dir, "config.yaml"),
        remote_path="/root/config.yaml",
        copy=True,
    )
)

volumes = {
    volume["path"]: Volume.from_name(volume["name"], create_if_missing=True)
    for volume in config["service"]["volumes"]
}

# ------------------------------------------------------------
# ServeFunction
# ------------------------------------------------------------


@app.function(
    image=vllm_image,
    gpu=f"{config['service']['gpu']}:{N_GPU}",
    scaledown_window=config["service"]["scaledown_window"]
    * MINUTES,  # how long should we stay up with no requests?
    timeout=config["service"]["timeout"]
    * MINUTES,  # how long should we wait for container start?
    volumes=volumes,
    min_containers=config["service"]["min_containers"],
    max_containers=config["service"]["max_containers"],
    secrets=[
        Secret.from_name(secret["name"]) for secret in config["service"]["secrets"]
    ],
)
@concurrent(  # how many requests can one replica handle? tune carefully!
    max_inputs=config["service"]["max_concurrent_requests"]
)
@web_server(
    port=VLLM_PORT, startup_timeout=config["service"]["startup_timeout"] * MINUTES
)
def serve():
    import json
    import subprocess
    import os

    vllm_api_key = os.environ.get("VLLM_API_KEY")
    hf_overrides = json.dumps(
        {
            "architectures": ["Qwen3VLForSequenceClassification"],
            "classifier_from_token": ["no", "yes"],
            "is_original_qwen3_reranker": True,
        }
    )

    cmd = [
        "vllm",
        "serve",
        MODEL_NAME,
        "--uvicorn-log-level",
        "info",
        "--revision",
        MODEL_REVISION,
        "--served-model-name",
        MODEL_NAME,
        "--host",
        "0.0.0.0",
        "--port",
        str(VLLM_PORT),
        "--runner",
        "pooling",
        "--max-model-len",
        str(MAX_MODEL_LEN),
        "--api-key",  # Pass the API key from the environment variable
        vllm_api_key,
        "--hf-overrides",
        hf_overrides,
        "--chat-template",
        os.path.join(local_dir, "qwen3_reranker.jinja"),
    ]

    # enforce-eager disables both Torch compilation and CUDA graph capture
    # default is no-enforce-eager. see the --compilation-config flag for tighter control
    cmd += ["--enforce-eager" if FAST_BOOT else "--no-enforce-eager"]

    # assume multiple GPUs are for splitting up large matrix multiplications
    cmd += ["--tensor-parallel-size", str(N_GPU)]

    print(*cmd)
    subprocess.Popen(cmd)
