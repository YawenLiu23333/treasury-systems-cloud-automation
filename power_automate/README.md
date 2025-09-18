# Power Automate Alert Hook (Optional)

**What:** Receive error alerts from the pipeline.

**Steps:**
1. In Power Automate (or Azure Logic Apps), create a **"When an HTTP request is received"** flow.
2. Paste a simple JSON schema like:
   ```json
   {
     "type": "object",
     "properties": {
       "env": { "type": "string" },
       "message": { "type": "string" }
     }
   }
   ```
3. After saving, copy the **HTTP POST URL** it generates.
4. Put that URL into your `.env` as `ALERT_WEBHOOK_URL=...`.
5. Trigger an error (e.g., break your DB URL), run `python main.py`, and confirm an alert arrives.

The app sends:
```json
{ "env": "local", "message": "Pipeline failure: <details>" }
```
