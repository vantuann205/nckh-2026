"""
Install Dependencies & Check Prerequisites
"""

import subprocess
import sys
import shutil
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("InstallDeps")


def check_python():
    v = sys.version_info
    logger.info(f"✅ Python {v.major}.{v.minor}.{v.micro}")
    if v.major < 3 or (v.major == 3 and v.minor < 8):
        logger.warning("⚠️ Python 3.8+ recommended")


def check_node():
    if shutil.which("node"):
        result = subprocess.run(["node", "--version"], capture_output=True, text=True)
        logger.info(f"✅ Node.js {result.stdout.strip()}")
    else:
        logger.warning("⚠️ Node.js not found — needed for frontend dev server")
        logger.info("   Download: https://nodejs.org/")


def check_redis():
    if shutil.which("redis-server") or shutil.which("redis-cli"):
        logger.info("✅ Redis found in PATH")
    else:
        logger.warning("⚠️ Redis not found in PATH")
        logger.info("   Windows: https://github.com/microsoftarchive/redis/releases")
        logger.info("   Or use: choco install redis-64")


def check_java():
    if shutil.which("java"):
        result = subprocess.run(["java", "-version"], capture_output=True, text=True)
        version = result.stderr.split("\n")[0] if result.stderr else result.stdout.split("\n")[0]
        logger.info(f"✅ Java: {version}")
    else:
        logger.warning("⚠️ Java not found — needed for Kafka")
        logger.info("   Download: https://adoptium.net/")


def install_python_deps():
    logger.info("\n📦 Installing Python dependencies...")
    subprocess.run([sys.executable, "-m", "pip", "install", "-r", "requirements.txt", "--quiet"])
    logger.info("✅ Python dependencies installed")


def install_frontend_deps():
    logger.info("\n📦 Installing frontend dependencies...")
    subprocess.run(["npm", "install"], cwd="dashboard", shell=True)
    logger.info("✅ Frontend dependencies installed")


def main():
    logger.info("=" * 50)
    logger.info("🔧 DEPENDENCY CHECK & INSTALL")
    logger.info("=" * 50)

    check_python()
    check_node()
    check_redis()
    check_java()

    install_python_deps()
    install_frontend_deps()

    logger.info("\n" + "=" * 50)
    logger.info("✅ SETUP COMPLETE")
    logger.info("=" * 50)
    logger.info("Next: python scripts/start_all.py")


if __name__ == "__main__":
    main()
