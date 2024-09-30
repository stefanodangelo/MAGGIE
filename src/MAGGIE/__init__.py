from dotenv import load_dotenv, find_dotenv

__version__ = "0.0.1"

# Load secrets for local development
_ = load_dotenv(find_dotenv('.env'))