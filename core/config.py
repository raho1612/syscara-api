import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Verzeichnisse entdecken
CURRENT_DIR = Path(__file__).resolve().parent.parent
def _discover_workspace_root() -> Path:
    for candidate in [CURRENT_DIR.parent, *CURRENT_DIR.parents]:
        if (candidate / "pyproject.toml").exists():
            return candidate
    return CURRENT_DIR.parent

WORKSPACE_ROOT = _discover_workspace_root()
ROOT_DIR = Path(os.getenv("APP_DATA_ROOT") or str(WORKSPACE_ROOT))

# .env laden
load_dotenv()
for env_path in (CURRENT_DIR / ".env", CURRENT_DIR.parent / ".env", WORKSPACE_ROOT / ".env"):
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=False)

# Shared Path Setup
SHARED_IMPORT_ROOTS = [
    CURRENT_DIR,
    CURRENT_DIR.parent,
    CURRENT_DIR.parent / "syscara-dashboard",
]

for shared_root in SHARED_IMPORT_ROOTS:
    if shared_root.exists() and str(shared_root) not in sys.path:
        sys.path.append(str(shared_root))

# Globale Konstanten
SYSCARA_BASE  = "https://api.syscara.com"
SYSCARA_USER  = os.getenv("SYSCARA_API_USER")
SYSCARA_PASS  = os.getenv("SYSCARA_API_PASS")
API_VERSION   = os.getenv("SYSCARA_API_VERSION", "v1.0.0")

# AI Flags
HAS_OPENAI = False
try:
    import openai
    HAS_OPENAI = True
except ImportError:
    pass

HAS_GEMINI = False
try:
    import google.generativeai
    HAS_GEMINI = True
except ImportError:
    pass

HAS_CLAUDE = False
try:
    import anthropic
    HAS_CLAUDE = True
except ImportError:
    pass
