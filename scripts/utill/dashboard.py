#!/usr/bin/env python3
# scripts/util/dashboard.py
# Flask 기반 웹 대시보드

from flask import Flask, jsonify, render_template_string
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent.parent.parent))

from scripts.collector import get_registered_collectors
from master_collectors import get_collector_stats
from datetime import datetime

app = Flask(__name__)

DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Hi-Dunkey 대시보드</title>
    <meta http-equiv="refresh" content="60">
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
        h1 { color: #333; }
        table { border-collapse: collapse; width: 100%; background: white; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        th, td { border: 1px solid #ddd; padding: 12px; text-align: left; }
        th { background-color: #4CAF50; color: white; }
        tr:nth-child(even) { background-color: #f9f9f9; }
        tr:hover { background-color: #f1f1f1; }
        .status-ok { color: green; font-weight: bold; }
        .status-warn { color: orange; font-weight: bold; }
        .status-error { color: red; font-weight: bold; }
        .refresh { color: #666; font-size: 12px; }
    </style>
</head>
<body>
    <h1>📊 Hi-Dunkey 중앙 대시보드</h1>
    <p class="refresh">마지막 업데이트: {{ timestamp }} (60 초 자동 새로고침)</p>
    <table>
        <tr>
            <th>수집기</th>
            <th>레코드</th>
            <th>크기 (MB)</th>
            <th>마지막수정</th>
            <th>상태</th>
        </tr>
        {% for row in rows %}
        <tr>
            <td>{{ row.name }}</td>
            <td>{{ row.records }}</td>
            <td>{{ row.size }}</td>
            <td>{{ row.modified }}</td>
            <td class="{{ row.status_class }}">{{ row.status }}</td>
        </tr>
        {% endfor %}
    </table>
</body>
</html>
"""


@app.route('/')
def dashboard():
    collectors = get_registered_collectors()
    rows = []
    
    for name in sorted(collectors.keys()):
        stats = get_collector_stats(name)
        if stats:
            status = '✅ 정상' if stats['total_records'] > 0 else '⚠️ 데이터없음'
            status_class = 'status-ok' if stats['total_records'] > 0 else 'status-warn'
            rows.append({
                'name': name,
                'records': f"{stats['total_records']:,}",
                'size': f"{stats['file_size_mb']:.2f}",
                'modified': stats['last_modified'].strftime('%Y-%m-%d %H:%M') if stats['last_modified'] else 'N/A',
                'status': status,
                'status_class': status_class
            })
    
    return render_template_string(DASHBOARD_HTML, rows=rows, timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'))


@app.route('/api/collectors')
def api_collectors():
    """API 엔드포인트 - 외부 연동용"""
    collectors = get_registered_collectors()
    data = []
    
    for name, cls in collectors.items():
        stats = get_collector_stats(name)
        data.append({
            'name': name,
            'table_name': getattr(cls, 'table_name', 'N/A'),
            'schema_name': getattr(cls, 'schema_name', 'N/A'),
            'description': getattr(cls, 'description', 'N/A'),
            'stats': stats
        })
    
    return jsonify(data)


if __name__ == '__main__':
    print("🌐 웹 대시보드 시작: http://localhost:5000")
    app.run(host='0.0.0.0', port=5000, debug=True)
