from flask import Flask, render_template, request, send_file, session, redirect, url_for, flash
import os
import re
from datetime import datetime
from werkzeug.utils import secure_filename

from auth import auth
from processor import (
    train_from_excel,
    extract_from_pdf,
    match_and_group,
    generate_pdf,
    process_sort_pipeline
)

app = Flask(__name__)

# -------------------------
# SESSION CONFIG
# -------------------------
app.secret_key = os.urandom(24)
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "LAX"

app.register_blueprint(auth, url_prefix="/auth")

UPLOAD_FOLDER = "uploads"
ACCOUNTS_FOLDER = "accounts"

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(ACCOUNTS_FOLDER, exist_ok=True)


# -------------------------
# CLEAN NAME
# -------------------------
def clean_name(name):
    return re.sub(r"[^a-z0-9]", "_", str(name).strip().lower())


# -------------------------
# GET USER ACCOUNTS
# -------------------------
def get_accounts(username):
    user_path = os.path.join(ACCOUNTS_FOLDER, username)
    if not os.path.exists(user_path):
        return []

    return [
        f for f in os.listdir(user_path)
        if os.path.isdir(os.path.join(user_path, f))
    ]


# -------------------------
# HOME PAGE
# -------------------------
@app.route("/", methods=["GET"])
def index():
    if "user" not in session:
        return redirect(url_for("auth.login"))

    username = session["user"]

    user_path = os.path.join(ACCOUNTS_FOLDER, username)
    os.makedirs(user_path, exist_ok=True)

    accounts = get_accounts(username)

    return render_template("index.html", accounts=accounts, user=username)


# ======================================================
# 📊 GENERATE REPORT
# ======================================================
@app.route("/generate-report", methods=["POST"])
def generate_report():

    if "user" not in session:
        return redirect(url_for("auth.login"))

    username = session["user"]

    # ACCOUNT
    account_select = request.form.get("account_select", "")
    new_account = request.form.get("new_account", "")

    if new_account.strip():
        account_name = clean_name(new_account)
    elif account_select.strip():
        account_name = clean_name(account_select)
    else:
        flash("❌ Select or create account")
        return redirect("/")

    # FILES
    manifest = request.files.get("manifest")
    train = request.files.get("train")

    if not manifest or manifest.filename == "":
        flash("❌ Manifest PDF required")
        return redirect("/")

    # PATHS
    user_path = os.path.join(ACCOUNTS_FOLDER, username)
    account_path = os.path.join(user_path, account_name)
    os.makedirs(account_path, exist_ok=True)

    upload_path = os.path.join(UPLOAD_FOLDER, username)
    os.makedirs(upload_path, exist_ok=True)

    # SAVE MANIFEST
    manifest_name = secure_filename(manifest.filename)
    manifest_path = os.path.join(upload_path, manifest_name)
    manifest.save(manifest_path)

    # TRAIN FILE
    train_path = os.path.join(account_path, "train.xlsx")

    if train and train.filename:
        train.save(train_path)

    if not os.path.exists(train_path):
        flash("❌ Upload training Excel first")
        return redirect("/")

    # PROCESS
    try:
        mapping = train_from_excel(train_path)
        manifest_data = extract_from_pdf(manifest_path)
        result = match_and_group(mapping, manifest_data)
    except Exception as e:
        return f"❌ Processing error: {str(e)}"

    # OUTPUT
    output_dir = os.path.join(account_path, "outputs")
    os.makedirs(output_dir, exist_ok=True)

    filename = f"{account_name}_report_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.pdf"
    output_path = os.path.join(output_dir, filename)

    generate_pdf(result, output_path)

    return send_file(output_path, as_attachment=True, download_name=filename)


# ======================================================
# 📦 SORT LABELS
# ======================================================
@app.route("/sort-labels", methods=["POST"])
def sort_labels():

    if "user" not in session:
        return redirect(url_for("auth.login"))

    username = session["user"]

    account_select = request.form.get("account_select", "").strip()

    if not account_select:
        flash("❌ Select account")
        return redirect("/")

    account_name = clean_name(account_select)

    label_file = request.files.get("label")

    if not label_file or label_file.filename == "":
        flash("❌ Label PDF required")
        return redirect("/")

    # MULTI COURIER
    selected_couriers = request.form.getlist("courier")
    if not selected_couriers:
        selected_couriers = None

    # PATHS
    account_path = os.path.join(ACCOUNTS_FOLDER, username, account_name)

    if not os.path.exists(account_path):
        flash("❌ Account not found")
        return redirect("/")

    excel_path = os.path.join(account_path, "train.xlsx")

    if not os.path.exists(excel_path):
        flash("❌ Training Excel missing")
        return redirect("/")

    # SAVE LABEL FILE
    upload_path = os.path.join(UPLOAD_FOLDER, username)
    os.makedirs(upload_path, exist_ok=True)

    label_name = secure_filename(label_file.filename)
    label_path = os.path.join(upload_path, label_name)
    label_file.save(label_path)

    # OUTPUT
    output_dir = os.path.join(account_path, "outputs")
    os.makedirs(output_dir, exist_ok=True)

    courier_part = (
        "_".join([c.lower().replace(" ", "") for c in selected_couriers])
        if selected_couriers else "all"
    )

    filename = f"{account_name}_sorted_{courier_part}_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.pdf"
    final_output_path = os.path.join(output_dir, filename)

    # PROCESS
    try:
        temp_output = process_sort_pipeline(
            label_path,
            excel_path,
            selected_couriers,
            output_dir=output_dir
        )

        if not os.path.exists(temp_output):
            return "❌ Output not generated"

        os.replace(temp_output, final_output_path)

    except Exception as e:
        return f"❌ Sorting error: {str(e)}"

    return send_file(final_output_path, as_attachment=True, download_name=filename)


# ======================================================
# 📥 DOWNLOAD TRAIN FILE
# ======================================================
@app.route("/download-train/<account>")
def download_train(account):

    if "user" not in session:
        return redirect(url_for("auth.login"))

    username = session["user"]
    account_name = clean_name(account)

    train_path = os.path.join(ACCOUNTS_FOLDER, username, account_name, "train.xlsx")

    if not os.path.exists(train_path):
        flash("❌ No training file found")
        return redirect("/")

    return send_file(train_path, as_attachment=True, download_name=f"{account_name}_train.xlsx")


# -------------------------
# LOGIN REDIRECT
# -------------------------
@app.route("/login")
def login_redirect():
    return redirect(url_for("auth.login"))


# -------------------------
# LOGOUT
# -------------------------
@app.route("/logout")
def logout():
    session.clear()
    flash("Logged out successfully")
    return redirect(url_for("auth.login"))


# -------------------------
# RUN
# -------------------------
if __name__ == "__main__":
    app.run(debug=True)