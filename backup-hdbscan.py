"""
Earthquake Data Analysis Web Application
Author: Jingyuan Chen 9629
Course: CSE 6332 Cloud Computing
Description: Flask web app for uploading, cleaning, and analyzing earthquake data
using Azure SQL Server and Blob Storage
"""
import os
import pandas as pd
import pyodbc
from flask import Flask, render_template, request, jsonify, flash, redirect, url_for
from werkzeug.utils import secure_filename
from azure.storage.blob import BlobServiceClient
import json
from datetime import datetime, timedelta
import math
# from sklearn.cluster import DBSCAN
import numpy as np
import hdbscan
# from dotenv import load_dotenv

# Load environment variables
# load_dotenv()

app = Flask(__name__)
app.secret_key = 'loveeveryone'  # Change this in production

# Azure Configuration - Replace with your actual values
AZURE_SQL_SERVER = os.getenv('AZURE_SQL_SERVE')
AZURE_SQL_DATABASE = os.getenv('AZURE_SQL_DATABASE')
AZURE_SQL_USERNAME = os.getenv('AZURE_SQL_USERNAME')
AZURE_SQL_PASSWORD = os.getenv('AZURE_SQL_PASSWORD')

AZURE_STORAGE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
BLOB_CONTAINER_NAME = os.getenv('BLOB_CONTAINER_NAME')

# Upload configuration
# UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'csv'}
# app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# Create upload directory if it doesn't exist
# os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def allowed_file(filename):
    """Check if uploaded file has allowed extension"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def get_db_connection():
    """Establish connection to Azure SQL Database"""
    try:
        connection_string = f"""
        DRIVER={{ODBC Driver 18 for SQL Server}};
        SERVER={AZURE_SQL_SERVER};
        DATABASE={AZURE_SQL_DATABASE};
        UID={AZURE_SQL_USERNAME};
        PWD={AZURE_SQL_PASSWORD};
        Encrypt=yes;
        TrustServerCertificate=no;
        Connection Timeout=30;
        """
        conn = pyodbc.connect(connection_string)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

def create_earthquake_table():
    """Create the earthquakes table if it doesn't exist"""
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            create_table_sql = """
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='earthquakes_511610' AND xtype='U')
            CREATE TABLE earthquakes_511610 (
                id NVARCHAR(50) PRIMARY KEY,
                time DATETIME2,
                latitude FLOAT,
                longitude FLOAT,
                depth FLOAT,
                mag FLOAT,
                place NVARCHAR(255),
                local_time DATETIME2,
                hour_of_day INT,
                day_of_week INT
            )
            """
            cursor.execute(create_table_sql)
            conn.commit()
            cursor.close()
            conn.close()
            return True
        except Exception as e:
            print(f"Error creating table: {e}")
            return False
    return False

def clean_earthquake_data(df):
    """Clean and prepare earthquake data for database insertion"""
    print(f"Original data shape: {df.shape}")
    
    # Required columns for our analysis
    required_columns = ['id', 'time', 'latitude', 'longitude', 'depth', 'mag', 'place']
    
    # Check if all required columns exist
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    # Select only required columns
    df_clean = df[required_columns].copy()
    
    # Remove rows with null values in critical fields
    df_clean = df_clean.dropna(subset=['id', 'time', 'latitude', 'longitude', 'mag'])
    
    # Convert time to datetime
    df_clean['time'] = pd.to_datetime(df_clean['time'], errors='coerce')
    df_clean = df_clean.dropna(subset=['time'])
    
    # Clean numeric fields
    df_clean['latitude'] = pd.to_numeric(df_clean['latitude'], errors='coerce')
    df_clean['longitude'] = pd.to_numeric(df_clean['longitude'], errors='coerce')
    df_clean['depth'] = pd.to_numeric(df_clean['depth'], errors='coerce')
    df_clean['mag'] = pd.to_numeric(df_clean['mag'], errors='coerce')
    
    # Remove rows with invalid coordinates or magnitude
    df_clean = df_clean.dropna(subset=['latitude', 'longitude', 'mag'])
    
    # Validate latitude and longitude ranges
    df_clean = df_clean[
        (df_clean['latitude'] >= -90) & (df_clean['latitude'] <= 90) &
        (df_clean['longitude'] >= -180) & (df_clean['longitude'] <= 180)
    ]
    
    # Fill missing depth with 0 (surface level)
    df_clean['depth'] = df_clean['depth'].fillna(0)
    
    # Fill missing place with 'Unknown Location'
    df_clean['place'] = df_clean['place'].fillna('Unknown Location')
    
    # Calculate derived fields
    df_clean['local_time'] = df_clean['time']  # Simplified - using UTC time
    df_clean['hour_of_day'] = df_clean['time'].dt.hour
    df_clean['day_of_week'] = df_clean['time'].dt.dayofweek + 1  # 1=Monday, 7=Sunday
    
    print(f"Cleaned data shape: {df_clean.shape}")
    return df_clean

def upload_to_database(df, batch_size=200):
    """Upload cleaned data to Azure SQL Database in batches (optimized)"""
    conn = get_db_connection()
    if not conn:
        return False, "Failed to connect to database"
    
    try:
        cursor = conn.cursor()
        cursor.fast_executemany = True  # ⚡加快插入速度

        cursor.execute("DELETE FROM earthquakes_511610")
        conn.commit()

        insert_sql = """
        INSERT INTO earthquakes_511610 
        (id, time, latitude, longitude, depth, mag, place, local_time, hour_of_day, day_of_week)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        # DataFrame to List of Tuples
        data_to_insert = [
            (
                str(row.id),        # 正确写法，使用属性访问
                row.time,
                float(row.latitude),
                float(row.longitude),
                float(row.depth),
                float(row.mag),
                str(row.place),
                row.local_time,
                int(row.hour_of_day),
                int(row.day_of_week)
            )
            for row in df.itertuples(index=False)
        ]

        # insert by batch
        total_rows = len(data_to_insert)
        uploaded_rows = 0

        for i in range(0, total_rows, batch_size):
            batch = data_to_insert[i:i + batch_size]
            cursor.executemany(insert_sql, batch)
            conn.commit()
            uploaded_rows += len(batch)
            print(f"Uploaded batch {i // batch_size + 1}: {uploaded_rows}/{total_rows} rows")

        cursor.close()
        conn.close()
        return True, f"Successfully uploaded {uploaded_rows} earthquake records"

    except Exception as e:
        print(f"Database upload error: {e}")
        return False, f"Database upload failed: {str(e)}"

def upload_to_blob_storage(df, filename):
    """Upload cleaned data to Azure Blob Storage"""
    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        
        # Create container if it doesn't exist
        try:
            blob_service_client.create_container(BLOB_CONTAINER_NAME)
        except:
            pass  # Container already exists
        
        # Convert DataFrame to CSV
        csv_data = df.to_csv(index=False)
        
        # Upload to blob
        blob_name = f"cleaned_{filename}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        blob_client = blob_service_client.get_blob_client(
            container=BLOB_CONTAINER_NAME, 
            blob=blob_name
        )
        
        blob_client.upload_blob(csv_data, overwrite=True)
        return True, blob_name
        
    except Exception as e:
        print(f"Blob storage error: {e}")
        return False, str(e)

def calculate_distance(lat1, lon1, lat2, lon2):
    """Calculate distance between two points using Haversine formula"""
    R = 6371  # Earth's radius in kilometers
    
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    
    a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    
    return R * c

@app.route('/')
def index():
    """Main page with upload form and analysis options"""
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    """Handle CSV file upload and processing"""
    if 'file' not in request.files:
        flash('No file selected')
        return redirect(request.url)
    
    file = request.files['file']
    if file.filename == '':
        flash('No file selected')
        return redirect(request.url)
    
    if file and allowed_file(file.filename):
        try:
            # Create table if it doesn't exist
            if not create_earthquake_table():
                flash('Failed to create database table')
                return redirect(url_for('index'))
            
            # Read and clean data
            df = pd.read_csv(file)
            df_clean = clean_earthquake_data(df)
            
            if df_clean.empty:
                flash('No valid data found after cleaning')
                return redirect(url_for('index'))
            
            # Upload to database
            success, message = upload_to_database(df_clean)
            if not success:
                flash(f'Database upload failed: {message}')
                return redirect(url_for('index'))
            
            # Upload to blob storage
            blob_success, blob_message = upload_to_blob_storage(df_clean, file.filename)
            if blob_success:
                flash(f'{message}. Cleaned data saved to blob storage as: {blob_message}')
            else:
                flash(f'{message}. Warning: Blob storage upload failed: {blob_message}')
            
            return redirect(url_for('index'))
            
        except Exception as e:
            flash(f'Error processing file: {str(e)}')
            return redirect(url_for('index'))
    
    flash('Invalid file type. Please upload a CSV file.')
    return redirect(url_for('index'))

@app.route('/api/earthquakes/magnitude_greater_than_5')
def earthquakes_magnitude_greater_than_5():
    """API endpoint: Count earthquakes with magnitude > 5.0"""
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM earthquakes_511610 WHERE mag > 5.0")
        count = cursor.fetchone()[0]
        
        cursor.execute("SELECT * FROM earthquakes_511610 WHERE mag > 5.0 ORDER BY mag DESC")
        earthquakes = []
        for row in cursor.fetchall():
            earthquakes.append({
                'id': row[0],
                'time': row[1].isoformat() if row[1] else None,
                'latitude': row[2],
                'longitude': row[3],
                'depth': row[4],
                'magnitude': row[5],
                'place': row[6]
            })
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'count': count,
            'earthquakes': earthquakes
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/earthquakes/magnitude_range')
def earthquakes_by_magnitude_range():
    """API endpoint: Search earthquakes by magnitude range and time period"""
    min_mag = float(request.args.get('min_mag', 2.0))
    max_mag = float(request.args.get('max_mag', 2.5))
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor = conn.cursor()
        
        # Build query with optional date filtering
        base_query = "SELECT COUNT(*) FROM earthquakes_511610 WHERE mag >= ? AND mag < ?"
        detail_query = "SELECT * FROM earthquakes_511610 WHERE mag >= ? AND mag < ?"
        params = [min_mag, max_mag]
        
        if start_date and end_date:
            base_query += " AND time >= ? AND time <= ?"
            detail_query += " AND time >= ? AND time <= ?"
            params.extend([start_date, end_date])
        
        detail_query += " ORDER BY mag DESC"
        
        # Get count
        cursor.execute(base_query, params)
        count = cursor.fetchone()[0]
        
        # Get detailed results
        cursor.execute(detail_query, params)
        earthquakes = []
        for row in cursor.fetchall():
            earthquakes.append({
                'id': row[0],
                'time': row[1].isoformat() if row[1] else None,
                'latitude': row[2],
                'longitude': row[3],
                'depth': row[4],
                'magnitude': row[5],
                'place': row[6]
            })
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'count': count,
            'magnitude_range': f"{min_mag} - {max_mag}",
            'date_range': f"{start_date} to {end_date}" if start_date and end_date else "All dates",
            'earthquakes': earthquakes
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/earthquakes/near_location')
def earthquakes_near_location():
    """API endpoint: Find earthquakes near a specified location"""
    try:
        target_lat = float(request.args.get('latitude'))
        target_lon = float(request.args.get('longitude'))
        radius_km = float(request.args.get('radius', 50))
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM earthquakes_511610")
        
        nearby_earthquakes = []
        for row in cursor.fetchall():
            eq_lat, eq_lon = row[2], row[3]
            distance = calculate_distance(target_lat, target_lon, eq_lat, eq_lon)
            
            if distance <= radius_km:
                nearby_earthquakes.append({
                    'id': row[0],
                    'time': row[1].isoformat() if row[1] else None,
                    'latitude': eq_lat,
                    'longitude': eq_lon,
                    'depth': row[4],
                    'magnitude': row[5],
                    'place': row[6],
                    'distance_km': round(distance, 2)
                })
        
        # Sort by distance
        nearby_earthquakes.sort(key=lambda x: x['distance_km'])
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'count': len(nearby_earthquakes),
            'target_location': {'latitude': target_lat, 'longitude': target_lon},
            'radius_km': radius_km,
            'earthquakes': nearby_earthquakes
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/earthquakes/clusters')
def earthquake_clusters():
    """API endpoint: Find earthquake clusters using HDBSCAN"""

    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT latitude, longitude, mag, time
            FROM earthquakes_511610
            WHERE mag >= 2.0
        """)
        rows = cursor.fetchall()

        if not rows:
            return jsonify({'error': 'No earthquake data found'}), 404

        # Prepare data for clustering
        coords = np.array([[row[0], row[1]] for row in rows])
        magnitudes = [row[2] for row in rows]
        times = [row[3] for row in rows]

        # HDBSCAN clustering
        kms_per_radian = 6371.0088
        min_cluster_size = 5

        clusterer = hdbscan.HDBSCAN(min_cluster_size=min_cluster_size, metric='haversine')
        labels = clusterer.fit_predict(np.radians(coords))

        unique_labels = set(labels)
        clusters = []

        for label in unique_labels:
            if label == -1:
                continue  # Skip noise

            indices = [i for i, l in enumerate(labels) if l == label]
            cluster_coords = coords[indices]
            cluster_mags = [magnitudes[i] for i in indices]
            cluster_times = [times[i] for i in indices]

            center_latitude = np.mean(cluster_coords[:, 0])
            center_longitude = np.mean(cluster_coords[:, 1])

            # Calculate cluster radius (max distance from center)
            distances = [
                calculate_distance(center_latitude, center_longitude, lat, lon)
                for lat, lon in cluster_coords
            ]
            cluster_radius = round(max(distances), 2) if distances else 0

            clusters.append({
                'center_latitude': round(center_latitude, 4),
                'center_longitude': round(center_longitude, 4),
                'earthquake_count': len(indices),
                'average_magnitude': round(np.mean(cluster_mags), 2),
                'max_magnitude': max(cluster_mags),
                'first_earthquake': min(cluster_times).isoformat() if cluster_times else None,
                'last_earthquake': max(cluster_times).isoformat() if cluster_times else None,
                'cluster_radius_km': cluster_radius,
            })

        # Get top 3 prone areas
        clusters.sort(key=lambda x: x['earthquake_count'], reverse=True)
        top_areas = clusters[:3] if len(clusters) >= 3 else clusters

        cursor.close()
        conn.close()

        return jsonify({
            'total_clusters': len(clusters),
            'top_prone_areas': top_areas,
            'all_clusters': clusters
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/earthquakes/night_analysis')
def large_earthquakes_night_analysis():
    """API endpoint: Analyze if large earthquakes (>4.0) occur more often at night"""
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor = conn.cursor()
        
        # Count large earthquakes by time of day
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN hour_of_day >= 20 OR hour_of_day < 6 THEN 'Night (20:00-05:59)'
                    WHEN hour_of_day >= 6 AND hour_of_day < 12 THEN 'Morning (06:00-11:59)'
                    WHEN hour_of_day >= 12 AND hour_of_day < 18 THEN 'Afternoon (12:00-17:59)'
                    ELSE 'Evening (18:00-19:59)'
                END as time_period,
                COUNT(*) as earthquake_count
            FROM earthquakes_511610 
            WHERE mag > 4.0
            GROUP BY 
                CASE 
                    WHEN hour_of_day >= 20 OR hour_of_day < 6 THEN 'Night (20:00-05:59)'
                    WHEN hour_of_day >= 6 AND hour_of_day < 12 THEN 'Morning (06:00-11:59)'
                    WHEN hour_of_day >= 12 AND hour_of_day < 18 THEN 'Afternoon (12:00-17:59)'
                    ELSE 'Evening (18:00-19:59)'
                END
            ORDER BY earthquake_count DESC
        """)
        
        time_distribution = []
        total_large_earthquakes = 0
        night_count = 0
        
        for row in cursor.fetchall():
            period_data = {
                'time_period': row[0],
                'count': row[1]
            }
            time_distribution.append(period_data)
            total_large_earthquakes += row[1]
            
            if 'Night' in row[0]:
                night_count = row[1]
        
        # Calculate hourly distribution for more detailed analysis
        cursor.execute("""
            SELECT hour_of_day, COUNT(*) as count
            FROM earthquakes_511610 
            WHERE mag > 4.0
            GROUP BY hour_of_day
            ORDER BY hour_of_day
        """)
        
        hourly_distribution = []
        for row in cursor.fetchall():
            hourly_distribution.append({
                'hour': row[0],
                'count': row[1]
            })
        
        # Calculate percentage
        night_percentage = (night_count / total_large_earthquakes * 100) if total_large_earthquakes > 0 else 0
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'total_large_earthquakes': total_large_earthquakes,
            'night_earthquakes': night_count,
            'night_percentage': round(night_percentage, 2),
            'occurs_more_at_night': night_percentage > 25,  # Night is 10 hours out of 24, so >41.7% would be "more often"
            'time_period_distribution': time_distribution,
            'hourly_distribution': hourly_distribution
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    # os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    app.run(debug=True)
