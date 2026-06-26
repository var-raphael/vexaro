package notification

import (
	"database/sql"
	"fmt"
	"log"
	//"os"

	//"github.com/resend/resend-go/v3"
)

var db *sql.DB

func Init(database *sql.DB) {
	db = database
}

// notify inserts an in-app notification for a user.
func Notify(userID string, datasetID *int64, kind string, message string) {
	if db == nil {
		log.Println("[notify] db not initialized")
		return
	}

	_, err := db.Exec(`
		INSERT INTO notifications (user_id, dataset_id, kind, message)
		VALUES (?, ?, ?, ?)
	`, userID, datasetID, kind, message)
	if err != nil {
		log.Printf("[notify] insert error user_id=%s kind=%s: %v", userID, kind, err)
	}
}

// NotifyEmail inserts an in-app notification and sends an email.
// It always calls Notify internally so in-app and email stay in sync.
/*  func NotifyEmail(userID string, datasetID *int64, kind string, message string, subject string, html string) {
	// Always insert in-app notification first
	if db == nil {
		log.Println("[notify-email] db not initialized")
		return
	}

	_, err := db.Exec(`
		INSERT INTO notifications (user_id, dataset_id, kind, message, emailed)
		VALUES (?, ?, ?, ?, 1)
	`, userID, datasetID, kind, message)
	if err != nil {
		log.Printf("[notify-email] insert error user_id=%s kind=%s: %v", userID, kind, err)
	}

	// Fetch user email
	var email string
	err = db.QueryRow(`
		SELECT email FROM user WHERE user_id = ?
	`, userID).Scan(&email)
	if err != nil {
		log.Printf("[notify-email] fetch email error user_id=%s: %v", userID, err)
		return
	}

	// Send via Resend
	apiKey := os.Getenv("RESEND_API_KEY")
	if apiKey == "" {
		log.Println("[notify-email] RESEND_API_KEY not set — skipping email")
		return
	}

	client := resend.NewClient(apiKey)
	params := &resend.SendEmailRequest{
		From:    "Vexaro <notifications@vexaro.dev>",
		To:      []string{email},
		Subject: subject,
		Html:    html,
	}

	sent, err := client.Emails.Send(params)
	if err != nil {
		log.Printf("[notify-email] send error user_id=%s kind=%s: %v", userID, kind, err)
		return
	}

	log.Printf("[notify-email] sent user_id=%s kind=%s resend_id=%s", userID, kind, sent.Id)
}

*/


// helpers to build clean email HTML — expand as needed

func emailBase(content string) string {
	return fmt.Sprintf(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Vexaro</title>
</head>
<body style="margin:0;padding:0;background-color:#0a0a0a;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
  <table width="100%%" cellpadding="0" cellspacing="0" style="background-color:#0a0a0a;padding:40px 20px;">
    <tr>
      <td align="center">
        <table width="100%%" cellpadding="0" cellspacing="0" style="max-width:560px;">

          <!-- Logo / Brand -->
          <tr>
            <td style="padding-bottom:32px;">
              <span style="font-size:20px;font-weight:700;color:#ffffff;letter-spacing:-0.5px;">
                vexaro
              </span>
              <span style="display:inline-block;width:6px;height:6px;background:#06b6d4;border-radius:50%%;margin-left:3px;vertical-align:middle;"></span>
            </td>
          </tr>

          <!-- Card -->
          <tr>
            <td style="background-color:#111111;border:1px solid #1f1f1f;border-radius:12px;padding:36px 36px 32px;">
              %s
            </td>
          </tr>

          <!-- Footer -->
          <tr>
            <td style="padding-top:24px;text-align:center;">
              <p style="margin:0;font-size:12px;color:#3f3f46;">
                You're receiving this because you have a Vexaro account.
                <br/>
                <a href="https://vexaro.dev" style="color:#52525b;text-decoration:none;">vexaro.dev</a>
              </p>
            </td>
          </tr>

        </table>
      </td>
    </tr>
  </table>
</body>
</html>`, content)
}

func FirstDatasetHTML(datasetName string) (subject, html string) {
	subject = "Your first dataset is ready 🎉"
	content := fmt.Sprintf(`
    <h2 style="margin:0 0 8px;font-size:22px;font-weight:700;color:#ffffff;letter-spacing:-0.3px;">
      First dataset created
    </h2>
    <p style="margin:0 0 24px;font-size:14px;color:#71717a;line-height:1.6;">
      Your dataset is being processed and will be ready shortly.
    </p>

    <div style="background-color:#18181b;border:1px solid #27272a;border-radius:8px;padding:16px 20px;margin-bottom:28px;">
      <p style="margin:0 0 4px;font-size:11px;font-weight:600;color:#52525b;text-transform:uppercase;letter-spacing:0.08em;">Dataset</p>
      <p style="margin:0;font-size:15px;font-weight:600;color:#ffffff;">%s</p>
    </div>

    <p style="margin:0 0 24px;font-size:14px;color:#71717a;line-height:1.6;">
      Once processing is complete your data will be available via your dashboard and API endpoint.
    </p>

    <a href="https://vexaro.dev/dashboard"
       style="display:inline-block;background-color:#06b6d4;color:#ffffff;font-size:14px;font-weight:600;text-decoration:none;padding:12px 24px;border-radius:8px;letter-spacing:-0.1px;">
      Go to Dashboard →
    </a>
`, datasetName)
	html = emailBase(content)
	return
}


func DatasetDeletedHTML(datasetName string) (subject, html string) {
	subject = fmt.Sprintf(`Dataset "%s" has been deleted`, datasetName)
	content := fmt.Sprintf(`
    <h2 style="margin:0 0 8px;font-size:22px;font-weight:700;color:#ffffff;letter-spacing:-0.3px;">
      Dataset deleted
    </h2>
    <p style="margin:0 0 24px;font-size:14px;color:#71717a;line-height:1.6;">
      The following dataset has been permanently deleted from your account.
    </p>

    <div style="background-color:#18181b;border:1px solid #27272a;border-radius:8px;padding:16px 20px;margin-bottom:28px;">
      <p style="margin:0 0 4px;font-size:11px;font-weight:600;color:#52525b;text-transform:uppercase;letter-spacing:0.08em;">Dataset</p>
      <p style="margin:0;font-size:15px;font-weight:600;color:#ffffff;">%s</p>
    </div>

    <div style="background-color:#071f23;border:1px solid #164e63;border-radius:8px;padding:14px 18px;margin-bottom:28px;">
      <p style="margin:0;font-size:13px;color:#67e8f9;line-height:1.6;">
        All versions, files, and API endpoints associated with this dataset are no longer available. This action cannot be undone.
      </p>
    </div>

    <p style="margin:0 0 24px;font-size:14px;color:#71717a;line-height:1.6;">
      If this wasn't you, please contact us immediately at
      <a href="mailto:support@vexaro.dev" style="color:#06b6d4;text-decoration:none;">support@vexaro.dev</a>.
    </p>

    <a href="https://vexaro.dev/dashboard"
       style="display:inline-block;background-color:#06b6d4;color:#ffffff;font-size:14px;font-weight:600;text-decoration:none;padding:12px 24px;border-radius:8px;letter-spacing:-0.1px;">
      Go to Dashboard →
    </a>
`, datasetName)
	html = emailBase(content)
	return
}


func NotifyEmail(userID string, datasetID *int64, kind string, message string, subject string, html string) {
	Notify(userID, datasetID, kind, message)
}