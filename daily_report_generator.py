from fpdf import FPDF
import os
from datetime import datetime

# Create reports folder if not exists
if not os.path.exists('Reports'):
    os.makedirs('Reports')

def generate_daily_report():
    today = datetime.now().strftime("%Y-%m-%d")
    report_file = f"Reports/Daily_Trade_Report_{today}.pdf"

    # Dummy Trades - Replace this part with actual trades later
    trades = [
        {"Symbol": "BANKNIFTY", "Action": "BUY", "Entry": 330, "Exit": 360, "Status": "Target Hit", "P_L": "+1500"},
        {"Symbol": "PFC", "Action": "BUY", "Entry": 5.25, "Exit": 6.5, "Status": "Target Hit", "P_L": "+650"}
    ]

    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", size=12)

    pdf.cell(200, 10, txt=f"Daily Trade Report - {today}", ln=True, align='C')
    pdf.ln(10)

    for trade in trades:
        pdf.cell(0, 10, txt=f"Symbol: {trade['Symbol']} | Action: {trade['Action']} | Entry: {trade['Entry']} | Exit: {trade['Exit']} | Status: {trade['Status']} | P/L: {trade['P_L']}", ln=True)

    pdf.output(report_file)
    print(f"✅ Daily Trade Report generated: {report_file}")

# Run this file manually for now
if __name__ == "__main__":
    generate_daily_report()
