from aiohttp import web
import ldahelper 
import json

async def handle(request):
  group_id = request.match_info.get('group_id', "all")
  try: 
    body = await ldahelper.start_lda(group_id)
  except ValueError as ve:
    raise web.HTTPInternalServerError()
  except Exception as e:
    raise web.HTTPInternalServerError()
  return web.Response(body=json.dumps(body))

app = web.Application()
# app.router.add_get('/', handle)
app.router.add_get('/{group_id}', handle)

web.run_app(app)