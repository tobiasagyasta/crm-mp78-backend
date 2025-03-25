from dotenv import load_dotenv
import os

def init_env():
    """Initialize environment variables from .env file"""
    # Load environment variables from .env file if it exists
    load_dotenv()

    # Ensure critical environment variables are set
    required_vars = ['DATABASE_URL', 'JWT_SECRET_KEY']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing_vars)}"
        )