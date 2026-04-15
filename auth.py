from flask import Blueprint, render_template, request, redirect, session
import json
import bcrypt
import os

auth = Blueprint("auth", __name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
USER_FILE = os.path.join(BASE_DIR, "user.json")


# -------------------------
# SAFE LOAD USERS (FIXED)
# -------------------------
def load_users():
    if not os.path.exists(USER_FILE):
        return {}

    try:
        with open(USER_FILE, "r") as f:
            content = f.read().strip()

            # EMPTY FILE SAFETY
            if not content:
                return {}

            return json.loads(content)

    except json.JSONDecodeError:
        # CORRUPTED FILE SAFETY
        return {}

    except Exception:
        return {}


# -------------------------
# SAFE SAVE USERS
# -------------------------
def save_users(data):
    with open(USER_FILE, "w") as f:
        json.dump(data, f, indent=4)


# -------------------------
# SIGNUP
# -------------------------
@auth.route("/signup", methods=["GET", "POST"])
def signup():
    if request.method == "POST":

        username = request.form["username"].strip()
        password = request.form["password"]

        if not username or not password:
            return "Username and password required"

        users = load_users()

        if username in users:
            return "❌ User already exists"

        hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

        users[username] = {
            "password": hashed
        }

        save_users(users)

        return redirect("/login")

    return render_template("signup.html")


# -------------------------
# LOGIN
# -------------------------
@auth.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":

        username = request.form["username"].strip()
        password = request.form["password"]

        users = load_users()

        if username not in users:
            return "❌ Invalid user"

        stored_hash = users[username]["password"]

        if bcrypt.checkpw(password.encode(), stored_hash.encode()):
            session["user"] = username
            return redirect("/")
        else:
            return "❌ Wrong password"

    return render_template("login.html")



# -------------------------
# LOGOUT
# -------------------------
@auth.route("/logout")
def logout():
    session.clear()
    return redirect("/auth/login")