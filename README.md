# CRM MP78 Backend

🚀 A Flask-based REST API for authentication and user management in a CRM system.

## Features

- 🔐 User authentication (Register & Login)
- 🔑 JWT-based authentication
- 📄 Protected routes
- 🗄 PostgreSQL database integration with SQLAlchemy
- 📦 Modular MVC structure

## Setup & Installation

1. **Clone the Repository**

   ```bash
   git clone https://github.com/your-username/crm-mp78-backend.git
   cd crm-mp78-backend
   ```

2. **Create and Activate Virtual Environment**

   **Mac/Linux:**

   ```bash
   python -m venv .venv
   source .venv/bin/activate
   ```

   **Windows:**

   ```powershell
   python -m venv .venv
   .venv\Scripts\activate
   ```

3. **Install Dependencies**

   ```bash
   pip install -r requirements.txt
   ```

4. **Configure Environment Variables**

   Create a `.env` file in the root directory and add:

   ```ini
   DATABASE_URL=postgresql://postgres:password@localhost:5432/crm_mp78_db
   JWT_SECRET_KEY=your-secret-key
   ```

5. **Initialize the Database**

   ```bash
   flask db init
   flask db migrate -m "Initial migration"
   flask db upgrade
   ```

6. **Run the Flask App**
   ```bash
   python -m app
   ```
   or (if using Flask CLI):
   ```bash
   flask run
   ```

## API Endpoints

| Method | Endpoint       | Description       | Auth Required |
| ------ | -------------- | ----------------- | ------------- |
| GET    | /              | Welcome message   | ❌ No         |
| POST   | /auth/register | User Registration | ❌ No         |
| POST   | /auth/login    | User Login (JWT)  | ❌ No         |
| GET    | /protected     | Protected Route   | ✅ Yes        |

## Project Structure

```bash
crm-mp78-backend/
│── app/
│   ├── __init__.py
│   ├── config/ # Configuration settings
│   ├── controllers/ # API route handlers
│   ├── models/ # Database models
│   ├── extensions/ # Extensions (DB, JWT)
│── migrations/ # Alembic migration files
│── .env # Environment variables
│── .gitignore
│── requirements.txt
│── README.md
```
