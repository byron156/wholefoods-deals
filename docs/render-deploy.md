# Render API Deploy

This deploys the Flask API in `app.py` so the Cloudflare frontend can use live shared data from Supabase.

## 1. Push the latest code

```bash
cd /Users/jonathancampbell/Code/wholefoods_deals
git add app.py requirements.txt render.yaml docs/render-deploy.md
git commit -m "Add Render deployment config for Flask API"
git push
```

## 2. Create the Render service

1. Go to [https://dashboard.render.com/](https://dashboard.render.com/)
2. Click `New +`
3. Click `Blueprint`
4. Connect the GitHub repo: `byron156/wholefoods-deals`
5. Render will detect `render.yaml`
6. Keep the service name or rename it
7. Create the blueprint

## 3. Add environment variables in Render

In the web service settings, set:

- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `PUBLIC_API_BASE_URL`
- `CORS_ALLOW_ORIGIN`

Recommended values:

- `SUPABASE_URL=https://YOUR_PROJECT_ID.supabase.co`
- `SUPABASE_SERVICE_ROLE_KEY=<your service role key>`
- `PUBLIC_API_BASE_URL=https://YOUR-RENDER-SERVICE.onrender.com`
- `CORS_ALLOW_ORIGIN=https://YOUR-CLOUDFLARE-SITE`

## 4. Verify the API

After deploy, check:

- `/health`
- `/api/feed?limit=5`
- `/api/profile?device_id=test-device`

Examples:

- `https://YOUR-RENDER-SERVICE.onrender.com/health`
- `https://YOUR-RENDER-SERVICE.onrender.com/api/feed?limit=5`

`/health` should return JSON with `"ok": true`.

## 5. Point the frontend at the API

Once the API URL is known, update the frontend build environment:

- `PUBLIC_API_BASE_URL=https://YOUR-RENDER-SERVICE.onrender.com`

Then rebuild and redeploy the static site so the homepage fetches live feed/profile/fix data from the API.

## 6. Tighten CORS

Once the frontend domain is final, replace `*` with your real Cloudflare site origin:

- `https://your-site.pages.dev`
- or your custom domain
