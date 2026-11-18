# CRM MP78 Backend

## Grab disbursement imports

Apply `sql/20261118_create_grab_disbursements.sql` before deploying the importer.
Upload CSV files as multipart form field `file` to
`POST /import-jobs/upload/grab-disbursement`; this uses the existing background
import-job endpoints for status and row issues. The disbursement ID is unique
across uploads. `Tanggal transfer` is stored as the transfer date; `Tanggal`,
bank name and bank statement code are not stored. The three- or four-digit
`Akun bank` value must match exactly one `rekenings.rekening_number` suffix;
unmatched and ambiguous rows are skipped with an import-job issue. Amounts are
expected as plain numeric values, as in Grab's CSV example.

The daily sheet and its preview show completed disbursements next to Grab Net.
They are matched by Grab store ID and summed on the transaction-date row for
the following calendar day's transfer date (H+1). A blank means no matching
completed disbursement was found; these are not bank-statement mutations.
The adjacent Grab Mutation column shows matched bank credits on the transfer
date and account, using the closest amount within Rp10,000. Ambiguous matches
are left blank and a bank mutation cannot be assigned to multiple payouts.
If only some payouts match, the column contains only their matched credits.

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
