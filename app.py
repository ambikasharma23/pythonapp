from flask import Flask, render_template, request, jsonify, send_file, redirect, url_for, session
import pandas as pd
import requests
from datetime import datetime
import time
import json
from urllib.parse import quote
import io
from werkzeug.utils import secure_filename
import logging
import codecs
from math import ceil
from datetime import timedelta
import tempfile
import os
import uuid
import atexit
from dotenv import load_dotenv
load_dotenv()
app = Flask(__name__)
app.secret_key = 'your-secret-key-here'
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(hours=1)

# Configuration
API_KEY = os.getenv("API_KEY")
#print(API_KEY)
BATCH_SIZE = 200
REQUEST_RATE = 4
DELAY_BETWEEN_BATCHES = 1 / REQUEST_RATE
STATUS_BASE_URL = "https://view.roambee.com/services/v2/autocrud/bee_commands"
SEND_URL = "https://view.roambee.com/services/command/send_commands"
ALLOWED_EXTENSIONS = {'xlsx', 'xls', 'csv'}

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure temporary storage
TEMP_UPLOAD_DIR = os.path.join(tempfile.gettempdir(), 'roambee_uploads')
os.makedirs(TEMP_UPLOAD_DIR, exist_ok=True)

# Cleanup function to remove temp files when app exits
def cleanup_temp_files():
    for filename in os.listdir(TEMP_UPLOAD_DIR):
        file_path = os.path.join(TEMP_UPLOAD_DIR, filename)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except Exception as e:
            logger.error(f"Failed to delete {file_path}: {e}")

atexit.register(cleanup_temp_files)

def clean_imei(imei):
    """Clean and validate IMEI"""
    if not isinstance(imei, str):
        imei = str(imei)
    imei = ''.join(c for c in imei if c.isdigit())
    return imei if len(imei) >= 12 else None

def get_imei_data():
    """Helper function to load IMEI data from temp file"""
    upload_id = session.get('imei_upload_id')
    if not upload_id:
        return None
    
    temp_path = os.path.join(TEMP_UPLOAD_DIR, f'{upload_id}.json')
    try:
        with open(temp_path, 'r') as f:
            return json.load(f)
    except:
        return None

@app.route('/')
def index():
    imei_data = get_imei_data()
    has_imeis = bool(imei_data and imei_data.get('imei_list', []))
    filename = imei_data.get('filename', '') if imei_data else ''
    imei_count = len(imei_data.get('imei_list', [])) if imei_data else 0
    
    return render_template('index.html',
                         has_imeis=has_imeis,
                         filename=filename,
                         imei_count=imei_count)

@app.route('/upload', methods=['POST'])
def upload_file():
    session.permanent = True
    
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400

    try:
        # Read file content
        file_content = file.read()
        
        try:
            if file.filename.lower().endswith('.csv'):
                file_content = file_content.decode('utf-8-sig').encode('utf-8')
                df = pd.read_csv(io.BytesIO(file_content))
            else:
                df = pd.read_excel(io.BytesIO(file_content))
        except Exception as e:
            logger.error(f"File read error: {str(e)}")
            return jsonify({'error': 'Invalid file format'}), 400
        
        logger.info(f"Processing file: {file.filename}")
        logger.info(f"Columns found: {df.columns.tolist()}")
        logger.info(f"First few rows:\n{df.head()}")
        
        # Find IMEI column (case insensitive)
        imei_col = None
        for col in df.columns:
            if 'imei' in col.lower():
                imei_col = col
                break
                
        if imei_col is None:
            logger.error("No IMEI column found")
            return jsonify({'error': 'No column containing "IMEI" found'}), 400
            
        # Process IMEIs with proper cleaning
        imei_list = df[imei_col].apply(clean_imei).dropna().unique().tolist()
        
        if not imei_list:
            logger.error("No valid IMEIs found after cleaning")
            return jsonify({'error': 'No valid IMEIs found in the file'}), 400
        
        # Generate unique ID for this upload
        upload_id = str(uuid.uuid4())
        temp_path = os.path.join(TEMP_UPLOAD_DIR, f'{upload_id}.json')
        
        # Save to temporary file
        with open(temp_path, 'w') as f:
            json.dump({
                'imei_list': imei_list,
                'filename': secure_filename(file.filename),
                'upload_time': datetime.now().isoformat()
            }, f)
        
        # Store just the reference in session
        session['imei_upload_id'] = upload_id
        
        logger.info(f"Successfully processed {len(imei_list)} IMEIs")
        logger.debug(f"Sample IMEIs: {imei_list[:5]}")

        return jsonify({
            'success': True,
            'imei_count': len(imei_list),
            'filename': secure_filename(file.filename)
        })
    
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        return jsonify({'error': f'Failed to process file: {str(e)}'}), 400

@app.route('/send_command', methods=['GET'])
def send_command_page():
    session.permanent = True
    if not get_imei_data():
        return redirect(url_for('index'))
    return render_template('send_command.html')

@app.route('/api/send_command', methods=['POST'])
def send_command():
    """Handle command sending with improved error handling"""
    imei_data = get_imei_data()
    if not imei_data or not imei_data.get('imei_list'):
        return jsonify({'error': 'No IMEI list uploaded'}), 400
    
    if not request.is_json:
        return jsonify({'error': 'Content-Type must be application/json'}), 415
    
    data = request.get_json()
    command = data.get('command', '').strip()
    
    if not command:
        return jsonify({'error': 'No command provided'}), 400
    
    imei_list = imei_data['imei_list']
    results = []
    total_imeis = len(imei_list)
    total_batches = ceil(total_imeis / BATCH_SIZE)
    
    headers = {
        "Content-Type": "application/json",
        "apikey": API_KEY
    }
    
    for batch_num in range(total_batches):
        start_idx = batch_num * BATCH_SIZE
        end_idx = min((batch_num + 1) * BATCH_SIZE, total_imeis)
        batch_imeis = imei_list[start_idx:end_idx]
        
        try:
            command_data = {
                "protocol": "WIRE",
                "imeis": batch_imeis,
                "commands": [command],
                "password": None
            }
            payload = {
                    "data": json.dumps(command_data)
                }
            
            logger.info(f"Sending command to {len(batch_imeis)} devices: {command}")
            
            response = requests.post(
                SEND_URL,
                headers=headers,
                json=payload,
                timeout=30
            )
            
            response_text = response.text.strip()
            status = "Failed"
            detailed_response = "No valid response from API"
            
            if response.status_code == 200:
                try:
                    response_data = response.json()
                    if isinstance(response_data, dict):
                        if "ids" in response_data:
                            status = "Success"
                            detailed_response = "Command queued successfully"
                        elif response_data.get("success", False):
                            status = "Success"
                            detailed_response = str(response_data)
                        else:
                            detailed_response = f"API returned unsuccessful response: {response_data}"
                    else:
                        detailed_response = f"Unexpected API response format: {response_data}"
                except ValueError:
                    if "success" in response_text.lower():
                        status = "Success"
                        detailed_response = response_text
                    else:
                        detailed_response = f"Invalid JSON response: {response_text}"
            else:
                detailed_response = f"API Error {response.status_code}: {response_text}"
            
            for imei in batch_imeis:
                results.append({
                    "IMEI": imei,
                    "Command": command,
                    "Status": status,
                    "Response": detailed_response,
                    "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
            
            time.sleep(DELAY_BETWEEN_BATCHES)
        
        except requests.exceptions.RequestException as e:
            error_msg = f"Request failed: {str(e)}"
            logger.error(error_msg)
            for imei in batch_imeis:
                results.append({
                    "IMEI": imei,
                    "Command": command,
                    "Status": "Error",
                    "Response": error_msg,
                    "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            logger.error(error_msg)
            for imei in batch_imeis:
                results.append({
                    "IMEI": imei,
                    "Command": command,
                    "Status": "Error",
                    "Response": error_msg,
                    "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
    
    output = io.BytesIO()
    df = pd.DataFrame(results)
    df.to_excel(output, index=False)
    output.seek(0)
    
    return send_file(
        output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=f'command_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
    )

@app.route('/check_status', methods=['GET'])
def check_status_page():
    session.permanent = True
    if not get_imei_data():
        return redirect(url_for('index'))
    return render_template('check_status.html')

@app.route('/api/check_status', methods=['POST'])
def check_status():
    session.permanent = True
    
    # Get IMEIs from temp file
    imei_data = get_imei_data()
    if not imei_data or not imei_data.get('imei_list'):
        return jsonify({'error': 'No IMEI list found'}), 400
    
    if not request.is_json:
        return jsonify({'error': 'Content-Type must be application/json'}), 415
    
    data = request.get_json()
    start_date = data.get('start_date')
    end_date = data.get('end_date')
    bulk_check = data.get('bulk_check', False)
    
    if not start_date or not end_date:
        return jsonify({'error': 'Both start_date and end_date are required'}), 400
    
    try:
        start_epoch = int(time.mktime(datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S").timetuple()))
        end_epoch = int(time.mktime(datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S").timetuple()))
    except ValueError:
        return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD HH:MM:SS'}), 400
    
    if start_epoch > end_epoch:
        return jsonify({'error': 'Start date must be before end date'}), 400
    
    imei_list = imei_data['imei_list']
    results = []
    total_imeis = len(imei_list)
    total_batches = ceil(total_imeis / BATCH_SIZE)
    
    status_counts = {
        "completed": 0,
        "pending": 0,
        "sent": 0,
        "acknowledged": 0,
        "failed": 0,
        "not_found": 0
    }
    
    for batch_num in range(total_batches):
        start_idx = batch_num * BATCH_SIZE
        end_idx = min((batch_num + 1) * BATCH_SIZE, total_imeis)
        batch_imeis = imei_list[start_idx:end_idx]
        
        try:
            filters = [
                {"name": "imei", "values": batch_imeis, "op": "in"},
                {"name": "created_date", "op": "gte", "value": start_epoch},
                {"name": "created_date", "op": "lte", "value": end_epoch},
                {"name": "imei", "isNull": False},
                {"name": "imei", "value": " ", "op": "ne"},
                {"name": "state", "values": [5], "op": "ne"}
            ]
            
            rbql = {
                "pagination": {"page_size": 500, "page_num": 1},
                "filters": filters,
                "sort": [{"name": "created_date", "order": "desc"}],
                "joins": [
                    {
                        "join_type": "left_join",
                        "table_name": "bees",
                        "left_table_attribute": "imei",
                        "right_table_attribute": "imei",
                        "fields": [
                            {"name": "bee_number", "readable_key": "Bee Number"},
                            {"name": "device_type", "readable_key": "Device Type"},
                            {"name": "uuid", "readable_key": "Bee UUID"}
                        ],
                        "filters": [
                            {"value": 1, "name": "active", "table_name": "bees"}
                        ]
                    },
                    {
                        "join_type": "left_join",
                        "table_name": "users",
                        "left_table_attribute": "request_by",
                        "right_table_attribute": "id",
                        "table_alias": "request_by",
                        "fields": [
                            {"name": "first_name", "readable_key": "Created By First Name"},
                            {"name": "last_name", "readable_key": "Created By Last Name"}
                        ]
                    }
                ]
            }
            
            rbql_str = json.dumps(rbql, separators=(',', ':'))
            rbql_encoded = quote(rbql_str)
            url = f"{STATUS_BASE_URL}?rbql={rbql_encoded}&isResellerAdmin=true"
            
            headers = {
                "Content-Type": "application/json",
                "apikey": API_KEY,
                "Accept": "application/json"
            }
            
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    if isinstance(data, dict) and data.get("total", 0) > 0:
                        imei_commands = {}
                        for command_data in data["data"]:
                            imei = command_data.get("imei")
                            if imei not in imei_commands:
                                imei_commands[imei] = []
                            imei_commands[imei].append(command_data)
                        
                        for imei in batch_imeis:
                            commands = imei_commands.get(imei, [])
                            
                            if not commands:
                                status_counts["not_found"] += 1
                                results.append({
                                    "IMEI": imei,
                                    "Sent Command": "N/A",
                                    "Status": "Not Found",
                                    "Message": "No commands in date range",
                                    "Created": "N/A",
                                    "Updated": "N/A",
                                    "Requested By": "N/A",
                                    "Device Type": "N/A",
                                    "Bee Number": "N/A"
                                })
                                continue
                            
                            if not bulk_check:
                                commands = [commands[0]]
                            
                            for cmd_idx, command_data in enumerate(commands):
                                state = command_data.get("state", -1)
                                
                                if state == 3:
                                    status_counts["completed"] += 1
                                    status = "Completed"
                                elif state in [4, 5]:
                                    status_counts["failed"] += 1
                                    status = "Failed"
                                elif state == 0:
                                    status_counts["pending"] += 1
                                    status = "Pending"
                                elif state == 1:
                                    status_counts["sent"] += 1
                                    status = "Sent"
                                elif state == 2:
                                    status_counts["acknowledged"] += 1
                                    status = "Acknowledged"
                                else:
                                    status_counts["failed"] += 1
                                    status = f"Unknown state ({state})"

                                device_type = command_data.get("bees__device_type", "N/A")
                                bee_number = command_data.get("bees__bee_number", "N/A")
                                raw_command = command_data.get("msg", "N/A")
                                at_command = extract_at_command(raw_command, device_type)
                                
                                results.append({
                                    "IMEI": imei,
                                    "Sent Command": at_command,
                                    "Status": status,
                                    "Message": command_data.get("error_message", ""),
                                    "Created": epoch_to_date(command_data.get("created_date")),
                                    "Updated": epoch_to_date(command_data.get("updated_date")),
                                    "Requested By": get_requester(command_data),
                                    "Device Type": device_type,
                                    "Bee Number": bee_number
                                })
                    else:
                        for imei in batch_imeis:
                            status_counts["not_found"] += 1
                            results.append({
                                "IMEI": imei,
                                "Sent Command": "N/A",
                                "Status": "Not Found",
                                "Message": "No commands in date range",
                                "Created": "N/A",
                                "Updated": "N/A",
                                "Requested By": "N/A",
                                "Device Type": "N/A",
                                "Bee Number": "N/A"
                            })
                except (ValueError, KeyError) as e:
                    for imei in batch_imeis:
                        status_counts["failed"] += 1
                        results.append({
                            "IMEI": imei,
                            "Sent Command": "N/A",
                            "Status": "Error",
                            "Message": f"Invalid response format: {str(e)}",
                            "Created": "N/A",
                            "Updated": "N/A",
                            "Requested By": "N/A",
                            "Device Type": "N/A",
                            "Bee Number": "N/A"
                        })
            else:
                for imei in batch_imeis:
                    status_counts["failed"] += 1
                    results.append({
                        "IMEI": imei,
                        "Sent Command": "N/A",
                        "Status": "Error",
                        "Message": f"API Error: {response.status_code} - {response.text[:100]}",
                        "Created": "N/A",
                        "Updated": "N/A",
                        "Requested By": "N/A",
                        "Device Type": "N/A",
                        "Bee Number": "N/A"
                    })
            
            time.sleep(DELAY_BETWEEN_BATCHES)
            
        except Exception as e:
            for imei in batch_imeis:
                status_counts["failed"] += 1
                results.append({
                    "IMEI": imei,
                    "Sent Command": "N/A",
                    "Status": "Error",
                    "Message": str(e),
                    "Created": "N/A",
                    "Updated": "N/A",
                    "Requested By": "N/A",
                    "Device Type": "N/A",
                    "Bee Number": "N/A"
                })
    
    output = io.BytesIO()
    df = pd.DataFrame(results)
    df.to_excel(output, index=False)
    output.seek(0)
    
    return send_file(
        output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=f'status_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
    )

def extract_at_command(msg, device_type=None):
    if not msg or msg == "N/A":
        return "N/A"
    
    if device_type in ["BSFlex", "BSMax", "BeeLabel", "BeeAssetFit"]:
        try:
            if len(msg) < 43:
                return "No AT command found"
            
            command_hex = msg[38:-4]
            if len(command_hex) % 2 != 0:
                command_hex = command_hex[:-1]
            
            try:
                decoded = bytes.fromhex(command_hex).decode('ascii')
                if decoded.startswith(("AT+", "at+")) or '=' in decoded:
                    return decoded
                return command_hex
            except:
                return command_hex
        except Exception as e:
            return f"Parse error: {str(e)}"
    return msg

def epoch_to_date(epoch):
    if epoch is None:
        return "N/A"
    try:
        return datetime.fromtimestamp(epoch).strftime("%Y-%m-%d %H:%M:%S")
    except:
        return "N/A"

def get_requester(item):
    if "request_by__first_name" in item and "request_by__last_name" in item:
        return f"{item['request_by__first_name']} {item['request_by__last_name']}"
    elif "request_by" in item:
        return str(item["request_by"])
    return "Unknown"

@app.route('/clear_imeis', methods=['POST'])
def clear_imeis():
    upload_id = session.pop('imei_upload_id', None)
    if upload_id:
        temp_path = os.path.join(TEMP_UPLOAD_DIR, f'{upload_id}.json')
        try:
            os.unlink(temp_path)
        except:
            pass
    return jsonify({'success': True})

if __name__ == '__main__':
    app.run(debug=True, port=5001)