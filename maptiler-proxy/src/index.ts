/**
 * Welcome to Cloudflare Workers! This is your first worker.
 *
 * - Run `npm run dev` in your terminal to start a development server
 * - Open a browser tab at http://localhost:8787/ to see your worker in action
 * - Run `npm run deploy` to publish your worker
 *
 * Bind resources to your worker in `wrangler.jsonc`. After adding bindings, a type definition for the
 * `Env` object can be regenerated with `npm run cf-typegen`.
 *
 * Learn more at https://developers.cloudflare.com/workers/
 */

interface Env {
    MAPTILER_API_KEY: string; // So TypeScript knows about your env variable
}

export default {
    async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
        const apiKey = env.MAPTILER_API_KEY;
        if (!apiKey) {
            return new Response("MapTiler API key is not configured", {
                status: 500,
                headers: { "Access-Control-Allow-Origin": "*" },
            });
        }

        const url = new URL(request.url);

        if (request.method === "OPTIONS") {
            return new Response(null, {
                status: 204, // No Content
                headers: {
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Methods": "GET,OPTIONS",
                    "Access-Control-Allow-Headers": "Content-Type",
                },
            });
        }

        // Match /tiles/:z/:x/:y.pbf
        const tileMatch = url.pathname.match(/^\/tiles\/(\d+)\/(\d+)\/(\d+)\.pbf$/);
        if (!tileMatch) {
            return new Response("Invalid tile request", {
                status: 400,
                headers: { "Access-Control-Allow-Origin": "*" },
            });
        }

        const [_, z, x, y] = tileMatch;

        // Construct MapTiler tile request URL
        const tileUrl = `https://api.maptiler.com/tiles/v3-lite/${z}/${x}/${y}.pbf?key=${apiKey}`;

        try {
            const response = await fetch(tileUrl, {
                method: "GET",
                headers: { "User-Agent": "Cloudflare Worker" },
            });

            if (!response.ok) {
                return new Response("Failed to fetch tile", {
                    status: response.status,
                    headers: { "Access-Control-Allow-Origin": "*" },
                });
            }

            return new Response(response.body, {
                status: 200,
                headers: {
                    "Content-Type": "application/x-protobuf",
                    "Access-Control-Allow-Origin": "*", // Or your domain
                    "Cache-Control": "public, max-age=86400", // Cache for 1 day
                },
            });
        } catch (error) {
            return new Response("Error fetching tile", {
                status: 500,
                headers: { "Access-Control-Allow-Origin": "*" },
            });
        }
    },
};
