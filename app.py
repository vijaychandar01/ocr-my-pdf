from flask import Flask, render_template, request, send_file, redirect, url_for, session, jsonify
import os
import ocrmypdf
from werkzeug.utils import secure_filename
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
import uuid
import PyPDF2
import tempfile
import shutil
import logging
from threading import Lock, Thread

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'your_secret_key')

app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['PROCESSED_FOLDER'] = 'processed'
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100 MB

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['PROCESSED_FOLDER'], exist_ok=True)

progress = {}
stop_signals = {}
progress_lock = Lock()

def process_single_pdf(file_path, output_path, language_flag, temp_folder, session_id, file_id, total_files):
    try:
        filename = os.path.basename(file_path)
        reader = PyPDF2.PdfReader(file_path)
        total_pages = len(reader.pages)

        with progress_lock:
            progress[session_id][file_id] = {
                'filename': filename,
                'total_pages': total_pages,
                'pages_done': 0,
                'ocr_progress': 0,
            }

        for page_num in range(total_pages):
            if stop_signals[session_id]:
                return None

            writer = PyPDF2.PdfWriter()
            writer.add_page(reader.pages[page_num])
            single_page_path = os.path.join(temp_folder, f"page_{page_num + 1}.pdf")

            with open(single_page_path, 'wb') as temp_pdf:
                writer.write(temp_pdf)

            ocrmypdf.ocr(single_page_path, single_page_path.replace('.pdf', '_ocr.pdf'), language=language_flag, skip_text=True)

            with progress_lock:
                progress[session_id][file_id]['ocr_progress'] = ((page_num + 1) / total_pages) * 100
                progress[session_id][file_id]['pages_done'] = page_num + 1

        if not stop_signals[session_id]:
            with open(output_path, 'wb') as output_pdf:
                writer = PyPDF2.PdfWriter()
                for page_num in range(total_pages):
                    processed_page_path = os.path.join(temp_folder, f"page_{page_num + 1}_ocr.pdf")
                    with open(processed_page_path, 'rb') as processed_pdf:
                        reader = PyPDF2.PdfReader(processed_pdf)
                        writer.add_page(reader.pages[0])
                writer.write(output_pdf)

        return output_path

    except Exception as e:
        app.logger.error(f"Error processing file {file_path}: {e}")
        return None


def process_pdfs(session_id, uploaded_file_paths, language_flag, max_workers=3):
    processed_files = []
    total_files = len(uploaded_file_paths)

    with progress_lock:
        progress[session_id] = {}

    stop_signals[session_id] = False

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for i, file_path in enumerate(uploaded_file_paths):
            if stop_signals[session_id]:
                break

            filename = os.path.basename(file_path)
            output_path = os.path.join(app.config['PROCESSED_FOLDER'], filename)
            temp_folder = tempfile.mkdtemp(dir=app.config['PROCESSED_FOLDER'])
            future = executor.submit(process_single_pdf, file_path, output_path, language_flag, temp_folder, session_id, i, total_files)
            futures.append(future)

        for future in as_completed(futures):
            result = future.result()
            if result:
                processed_files.append(result)

    if processed_files:
        zip_filename = 'processed_pdfs.zip'
        zip_filepath = os.path.join(app.config['PROCESSED_FOLDER'], zip_filename)
        with zipfile.ZipFile(zip_filepath, 'w') as zipf:
            for processed_file in processed_files:
                zipf.write(processed_file, os.path.basename(processed_file))
                os.remove(processed_file)

        with progress_lock:
            for file_id in progress[session_id]:
                progress[session_id][file_id]['ocr_progress'] = 100
    else:
        with progress_lock:
            for file_id in progress[session_id]:
                progress[session_id][file_id]['ocr_progress'] = 0


@app.route('/')
def index():
    session_id = str(uuid.uuid4())
    session['session_id'] = session_id
    return render_template('index.html', session_id=session_id)


@app.route('/upload', methods=['POST'])
def upload():
    session_id = session.get('session_id')
    uploaded_files = request.files.getlist("file[]")
    selected_languages = request.form.getlist('languages')

    if not selected_languages:
        return "Please select at least one language.", 400

    language_flag = '+'.join(selected_languages)
    uploaded_file_paths = []

    for file in uploaded_files:
        if file.filename.endswith('.pdf'):
            filename = secure_filename(file.filename)
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)
            uploaded_file_paths.append(file_path)

    # Specify the number of concurrent PDFs being processed here
    max_workers = 2

    thread = Thread(target=process_pdfs, args=(session_id, uploaded_file_paths, language_flag, max_workers))
    thread.start()

    return redirect(url_for('progress_page', session_id=session_id))


@app.route('/progress/<session_id>')
def progress_page(session_id):
    with progress_lock:
        all_files_done = all(file_data.get('ocr_progress', 0) == 100 for file_data in progress.get(session_id, {}).values())

    if all_files_done:
        return redirect(url_for('download', session_id=session_id))

    return render_template('progress.html', session_id=session_id)

@app.route('/status/<session_id>')
def status(session_id):
    with progress_lock:
        return jsonify(progress.get(session_id, {}))

@app.route('/stop/<session_id>', methods=['POST'])
def stop(session_id):
    stop_signals[session_id] = True
    return jsonify({'status': 'stopped'})

@app.route('/download/<session_id>')
def download(session_id):
    zip_filepath = os.path.join(app.config['PROCESSED_FOLDER'], 'processed_pdfs.zip')
    files_processed = progress.get(session_id, {})

    if not files_processed:
        return redirect(url_for('index'))

    all_files_done = all(file_data.get('ocr_progress', 0) == 100 for file_data in files_processed.values())

    if not all_files_done:
        return render_template('partial_download.html', session_id=session_id)

    if os.path.exists(zip_filepath):
        return send_file(zip_filepath, as_attachment=True)

    return redirect(url_for('index'))

@app.route('/partial-download/<session_id>', methods=['POST'])
def partial_download(session_id):
    download_choice = request.form.get('download_choice')
    zip_filepath = os.path.join(app.config['PROCESSED_FOLDER'], 'processed_pdfs.zip')

    if download_choice == 'yes' and os.path.exists(zip_filepath):
        return send_file(zip_filepath, as_attachment=True)

    return redirect(url_for('index'))

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 80))
    app.run(debug=True, host='0.0.0.0')
    
if __name__ != '__main__':
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)

app.logger.info('App started')
