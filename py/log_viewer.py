import http.server
import socketserver
import re

class LogViewerRequestHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/':
            log_data = self.parse_log_file()
            html = self.generate_html(log_data)
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(html.encode('utf-8'))
        else:
            super().do_GET()

    def parse_log_file(self):
        log_data = []
        with open('app.log', 'r') as file:
            lines = file.readlines()
            for line in lines:
                match = re.match(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) - (\w+) - pid: (\d+) server#(\d) (.+)', line)
                if match:
                    timestamp = match.group(1)
                    server_pid = match.group(3)
                    server_num = match.group(4)
                    message = match.group(5)
                    log_data.append({
                        'timestamp': timestamp.split(' ')[1],
                        'server_pid': server_pid,
                        'server_num': server_num,
                        'message': message
                    })

        print(f'length of log_data: {len(log_data)}')
        return log_data

    def generate_html(self, log_data):
        table_header = ['<th>Time</th>']
        servers = []
        for log_entry in log_data:
            sid = log_entry['server_num']
            svr = f"server#{log_entry['server_num']}-{log_entry['server_pid']}"
            if (sid, svr) not in servers:
                servers.append((sid, svr))
        servers = sorted(servers, key=lambda x: x[0])
        for e in servers:
            table_header.append(f"<th>{e[1]}</th>")
        table_header = ''.join(table_header)

        table_rows = ''
        for log_entry in log_data:
            html_row = f'<tr><td>{log_entry["timestamp"]}</td>'

            tds = ['<td></td>'] * len(servers)

            sn = int(log_entry['server_num'])
            tds[sn] = f'<td>{log_entry["message"]}</td>'
            html_row += ''.join(tds)

            # if log_entry['server_num'] == '0':
            #     html_row += f'<td>{log_entry["message"]}</td><td></td><td></td>'
            # if log_entry['server_num'] == '1':
            #     html_row += f'<td></td><td>{log_entry["message"]}</td><td></td>'
            # if log_entry['server_num'] == '2':
            #     html_row += f'<td></td><td></td><td>{log_entry["message"]}</td>'

            html_row += '</tr>'
            table_rows += html_row

        html = f'''
        <!DOCTYPE html>
        <html>
        <head>
            <title>Log Viewer</title>
            <style>
                table {{
                    border-collapse: collapse;
                }}
                
                th, td {{
                    border: 1px solid black;
                    padding: 8px;
                }}
            </style>
        </head>
        <body>
            <table>
                <tr> {table_header} </tr>
                {table_rows}
            </table>
        </body>
        </html>
        '''
        return html

PORT = 8887

with socketserver.TCPServer(("", PORT), LogViewerRequestHandler) as httpd:
    print(f"Server started on port {PORT}")
    httpd.serve_forever()
