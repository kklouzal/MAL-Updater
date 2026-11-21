from __future__ import annotations

from .web_shell import FAVICON_LINK, SHELL_CSS, top_bar

_SETTINGS_CSS = """
.panel{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:clamp(1rem,2.5vw,1.6rem);margin:0 0 1.25rem}
.panel>h2{font-size:1.3rem;margin-bottom:.35rem}
.panel-intro{margin:0 0 1rem;color:var(--muted);max-width:60rem}
.pill{display:inline-block;border-radius:999px;padding:.15rem .6rem;background:var(--surface-3);color:var(--muted);font-size:.85rem;margin-bottom:.75rem}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(16rem,100%),1fr));gap:1rem}
.tile h3{font-size:.8rem;text-transform:uppercase;letter-spacing:.05em;color:var(--muted);margin-bottom:.35rem}
.tile p{margin:0;font-weight:650}
.status-list{list-style:none;margin:0;padding:0;display:grid;gap:.3rem}
.status-list li{display:flex;align-items:baseline;gap:.5rem}
.dot{flex:none;width:.6rem;height:.6rem;border-radius:50%;background:var(--warn);transform:translateY(-.05rem)}
.dot.ok{background:var(--good)}
.status-detail{color:var(--muted);font-weight:400}
.credential-card{background:var(--surface-2);border:1px solid var(--border);border-radius:var(--radius);padding:1rem 1.1rem;min-width:0}
.credential-card h3{font-size:1.1rem;margin-bottom:.25rem}
label{display:block;margin:.85rem 0 .3rem;font-weight:600}
input{display:block;width:100%;margin-top:.3rem;padding:.6rem .7rem;border-radius:.45rem;border:1px solid var(--border-strong);background:var(--bg);color:var(--text);font:inherit;font-weight:400}
input:focus-visible{outline:3px solid var(--accent);outline-offset:1px}
.actions{display:flex;flex-wrap:wrap;gap:.5rem;margin-top:1rem}
.actions .button{flex:1 1 auto}
.message{margin:0;color:var(--good);font-weight:600}
.message.error{color:var(--bad)}
small{display:block;color:var(--muted);font-weight:400;font-size:.84rem}
.credential-status,.test-result{margin-top:.3rem}
.stored,.test-ok{color:var(--good)}
.not-stored,.test-failed{color:var(--bad)}
details{border:1px solid var(--border);border-radius:var(--radius);margin:.8rem 0;padding:.65rem 1rem;background:var(--surface-2)}
summary{cursor:pointer;font-weight:700}
.settings-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(17rem,100%),1fr));gap:0 1rem}
.setting{min-width:0}
.restart{margin:0 0 1rem;border-left:4px solid var(--accent);padding:.1rem 0 .1rem .8rem}
.link-panel{border:1px solid var(--border-strong);border-radius:var(--radius);padding:.8rem 1rem;margin-top:.8rem}
.link-code{font:800 2.1rem/1.2 ui-monospace,monospace;letter-spacing:.3em;text-transform:uppercase;margin:.3rem 0}
.link-or{margin-top:1rem}
"""

_SETTINGS_SCRIPT = r"""
let csrf='';const $=x=>document.querySelector(x),msg=(t,e=false)=>{let m=$('#msg');m.textContent=t;m.className=e?'message error':'message'},credential=(id,present)=>{let e=$(id);e.textContent=present?'Stored':'Not stored';e.className=`credential-status ${present?'stored':'not-stored'}`},testResult=(kind,result)=>{let e=$(`#${kind}TestResult`);if(!result){e.textContent='Not tested in this server session';e.className='test-result';return}e.textContent=result.message;e.className=`test-result ${result.ok?'test-ok':'test-failed'}`};
function connectionItem(label,ok,detail){let li=document.createElement('li'),dot=document.createElement('span'),text=document.createElement('span'),info=document.createElement('span');dot.className=`dot${ok?' ok':''}`;dot.setAttribute('aria-hidden','true');text.textContent=`${label}: `;info.className='status-detail';info.textContent=detail;text.append(info);li.append(dot,text);return li}
async function api(path,body){if(body&&!csrf)await bootstrap();let r=await fetch(path,{method:body?'POST':'GET',headers:body?{'Content-Type':'application/json','X-CSRF-Token':csrf}:{},body:body?JSON.stringify(body):null,cache:'no-store'});let j=await r.json().catch(()=>({error:'Unexpected response'}));if(!r.ok)throw Error((j.error||'Request failed').replaceAll('_',' '));return j}
async function bootstrap(){let r=await fetch('/api/csrf',{cache:'no-store'}),j=await r.json();if(!r.ok||!j.csrf_token)throw Error('Unable to initialize request protection');csrf=j.csrf_token}
const valueAt=(value,path)=>path.split('.').reduce((item,key)=>item[key],value);
function renderSettings(status){let root=$('#settingsFields');root.replaceChildren();let groups=new Map;for(let spec of status.settings_schema){if(!groups.has(spec.group))groups.set(spec.group,[]);groups.get(spec.group).push(spec)}for(let [name,specs] of groups){let details=document.createElement('details');details.open=['Scheduler cadence','Hourly request budgets','Per-run execution bounds','Provider pagination'].includes(name);let summary=document.createElement('summary');summary.textContent=name;details.append(summary);let grid=document.createElement('div');grid.className='settings-grid';for(let spec of specs){let label=document.createElement('label');label.className='setting';label.append(document.createTextNode(spec.label));let input=document.createElement('input');input.type='number';input.name=spec.path;input.min=spec.min;input.max=spec.max;input.step=spec.step;input.required=true;input.dataset.kind=spec.kind;input.value=valueAt(status.settings,spec.path);label.append(input);let help=document.createElement('small');help.textContent=spec.help+(spec.unit?` Unit: ${spec.unit}.`:'');label.append(help);grid.append(label)}details.append(grid);root.append(details)}}
async function refresh(){let [s,r]=await Promise.all([api('/api/settings'),api('/api/status')]);$('#setupState').textContent=s.setup_complete?'Complete':'Needs MAL configuration and OAuth';$('#automationState').textContent=r.automation_state==='running'?'Running':s.setup_complete?'Blocked while scheduler starts or restarts':'Blocked until MAL setup is complete';$('#connections').replaceChildren(connectionItem('MAL account',s.mal_oauth_complete,s.mal_oauth_complete?'connected':'not connected'),connectionItem('Crunchyroll sync',s.providers.crunchyroll.scheduler_eligible,s.providers.crunchyroll.scheduler_eligible?'active':'waiting for a linked account or both credentials'),connectionItem('HIDIVE sync',s.providers.hidive.scheduler_eligible,s.providers.hidive.scheduler_eligible?'active':'waiting for both credentials'));credential('#malClientIdStatus',s.secrets_present.mal_client_id);credential('#malClientSecretStatus',s.secrets_present.mal_client_secret);credential('#crunchyrollUsernameStatus',s.secrets_present.crunchyroll_username);credential('#crunchyrollPasswordStatus',s.secrets_present.crunchyroll_password);credential('#hidiveUsernameStatus',s.secrets_present.hidive_username);credential('#hidivePasswordStatus',s.secrets_present.hidive_password);for(let kind of ['mal','crunchyroll','hidive'])testResult(kind,s.connection_tests[kind]);renderSettings(s);if(s.settings_error)msg(s.settings_error,true)}
document.addEventListener('submit',async e=>{e.preventDefault();let f=e.target;try{if(f.dataset.action==='settings'){let values={};for(let input of f.querySelectorAll('input[name]'))values[input.name]=input.dataset.kind==='int'?Number.parseInt(input.value,10):Number(input.value);let saved=await api('/api/settings',values);msg(saved.restart_required?'Operational limits saved. Restart the scheduler/container to apply them.':'Operational limits saved.');renderSettings(saved);return}let d=Object.fromEntries(new FormData(f)),replace={};for(let[k,v]of Object.entries(d))if(v)replace[k]=v;if(!Object.keys(replace).length){msg('Nothing to save: enter a new value first.',true);return}await api('/api/secrets',{replace,remove:[]});f.reset();msg('Credentials saved. They were not tested.');refresh()}catch(x){msg(x.message,true)}});
$('#oauth').onclick=async()=>{try{let j=await api('/api/oauth/mal/start',{});location.href=j.authorization_url}catch(e){msg(e.message,true)}};
let crPoll=null;const crStop=t=>{clearInterval(crPoll);crPoll=null;if(t!==undefined)$('#crLinkStatus').textContent=t};
$('#crLink').onclick=async()=>{let tab=window.open('about:blank','_blank');try{if(crPoll)crStop();let j=await api('/api/crunchyroll/link/start',{});$('#crCode').textContent=j.user_code;$('#crLinkOpen').href=j.activation_url;$('#crLinkPanel').hidden=false;if(tab){tab.opener=null;tab.location.href=j.activation_url}let deadline=Date.now()+j.expires_in*1000,busy=false;const tick=async()=>{let left=Math.max(0,Math.round((deadline-Date.now())/1000));$('#crLinkStatus').textContent=`Waiting for approval… code expires in ${Math.floor(left/60)}:${String(left%60).padStart(2,'0')}`;if(left<=0){crStop('The code expired. Click Link Crunchyroll account for a new one.');return}if(busy)return;busy=true;try{let r=await api('/api/crunchyroll/link/poll',{flow_id:j.flow_id});if(r.status==='linked'){crStop('');$('#crLinkPanel').hidden=true;msg(`Crunchyroll account linked${r.account?` (${r.account})`:''}. Syncing resumes automatically.`);refresh()}else if(r.status==='expired')crStop('The code expired. Click Link Crunchyroll account for a new one.');else if(r.status==='error')crStop(r.message)}catch(e){crStop(e.message)}finally{busy=false}};crPoll=setInterval(tick,5000);tick()}catch(e){if(tab)tab.close();msg(e.message,true)}};
document.querySelectorAll('.test').forEach(b=>b.onclick=()=>test(b.dataset.provider));$('#malTest').onclick=()=>test('mal');
async function test(kind){try{let j=await api('/api/connections/test',{kind});msg(j.message);await refresh()}catch(e){msg(e.message,true);await refresh().catch(()=>{})}}
bootstrap().then(refresh).catch(e=>msg(e.message,true));
"""


def _credential_field(label: str, name: str, status_id: str, *, secret: bool, autocomplete: str) -> str:
    kind = ' type="password"' if secret else ""
    return (
        f'<label>{label}<input name="{name}"{kind} autocomplete="{autocomplete}">'
        f'<small id="{status_id}" class="credential-status"></small></label>'
    )


def page() -> str:
    mal_form = (
        '<form class="credential-card" data-action="secrets"><h3>MyAnimeList</h3>'
        "<small>Your MAL API application. Saved values are never shown; leave a field blank to keep it.</small>"
        + _credential_field("Client ID", "mal_client_id", "malClientIdStatus", secret=False, autocomplete="off")
        + _credential_field("Client secret", "mal_client_secret", "malClientSecretStatus", secret=True, autocomplete="new-password")
        + '<div class="actions"><button class="button primary">Save credentials</button>'
        '<button type="button" class="button" id="malTest">Test MAL connection</button></div>'
        '<small id="malTestResult" class="test-result" role="status" aria-live="polite">Not tested in this server session</small>'
        '<div class="actions"><button type="button" class="button" id="oauth">Connect MAL account</button></div></form>'
    )
    crunchyroll_form = (
        '<form class="credential-card" data-action="secrets"><h3>Crunchyroll</h3>'
        "<small>Recommended: link your account without a password. Make sure you are signed in at crunchyroll.com in this browser first.</small>"
        '<div class="actions"><button type="button" class="button primary" id="crLink">Link Crunchyroll account</button></div>'
        '<div id="crLinkPanel" class="link-panel" hidden><p>Approve this code on the Crunchyroll tab that opened:</p>'
        '<p id="crCode" class="link-code"></p><a id="crLinkOpen" target="_blank" rel="noopener noreferrer">Open Crunchyroll activation page</a>'
        '<small id="crLinkStatus" role="status" aria-live="polite"></small></div>'
        '<small class="link-or">Or sign in with a username and password:</small>'
        + _credential_field("Username", "crunchyroll_username", "crunchyrollUsernameStatus", secret=False, autocomplete="username")
        + _credential_field("Password", "crunchyroll_password", "crunchyrollPasswordStatus", secret=True, autocomplete="new-password")
        + '<div class="actions"><button class="button">Save Crunchyroll</button>'
        '<button type="button" class="button test" data-provider="crunchyroll">Test Crunchyroll</button></div>'
        '<small id="crunchyrollTestResult" class="test-result" role="status" aria-live="polite">Not tested in this server session</small></form>'
    )
    hidive_form = (
        '<form class="credential-card" data-action="secrets"><h3>HIDIVE</h3>'
        "<small>Your HIDIVE sign-in, used to read watch history.</small>"
        + _credential_field("Username", "hidive_username", "hidiveUsernameStatus", secret=False, autocomplete="username")
        + _credential_field("Password", "hidive_password", "hidivePasswordStatus", secret=True, autocomplete="new-password")
        + '<div class="actions"><button class="button primary">Save HIDIVE</button>'
        '<button type="button" class="button test" data-provider="hidive">Test HIDIVE</button></div>'
        '<small id="hidiveTestResult" class="test-result" role="status" aria-live="polite">Not tested in this server session</small></form>'
    )
    return (
        '<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
        f'<meta name="color-scheme" content="dark">{FAVICON_LINK}<title>MAL-Updater settings</title><style>{SHELL_CSS}{_SETTINGS_CSS}</style></head><body>'
        + top_bar("settings")
        + '<main class="page"><div class="page-head"><h1>Settings</h1>'
        '<p id="msg" class="message" role="status" aria-live="polite"></p></div>'
        '<section id="dashboard" class="panel" aria-labelledby="overview-title"><span class="pill">Trusted LAN control plane</span>'
        '<h2 id="overview-title">Configuration</h2><div class="grid">'
        '<div class="tile"><h3>Setup</h3><p id="setupState"></p></div>'
        '<div class="tile"><h3>Automation</h3><p id="automationState"></p><small>The scheduler starts automatically when MAL setup is complete.</small></div>'
        '<div class="tile"><h3>Connections</h3><ul id="connections" class="status-list"></ul></div></div>'
        '<p><small>MAL writes remain conservative and are not enabled merely by onboarding.</small></p></section>'
        '<section id="settings" class="panel" aria-labelledby="credentials-title"><h2 id="credentials-title">Credentials</h2>'
        f'<div class="grid">{mal_form}{crunchyroll_form}{hidive_form}</div></section>'
        '<section id="limits" class="panel" aria-labelledby="limits-title"><h2 id="limits-title">Operational limits</h2>'
        '<p class="panel-intro">Customize request budgets, per-run bounds, pagination, pacing, retries, and learned-budget backoff. '
        "Values are numeric and bounded; this is not an arbitrary TOML editor.</p>"
        '<p class="restart"><strong>Restart required.</strong> Saved changes apply after the scheduler/container restarts. '
        "The running scheduler and provider clients are not live-reloaded or mutated.</p>"
        '<form id="settingsForm" data-action="settings"><div id="settingsFields"></div>'
        '<div class="actions"><button class="button primary">Save operational limits</button></div></form></section></main>'
        f"<script>{_SETTINGS_SCRIPT}</script></body></html>"
    )
