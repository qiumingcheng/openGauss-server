"""Backend-only feature macros and actual DWARF layout gate for test modules.

pg_config does not report Makefile.global's conditional CPPFLAGS overrides.
Do not load a module touching t_thrd/u_sess until its measured layouts match
the running native binary. This file changes no product build flags.
"""
import json
from pathlib import Path
import re
import subprocess

def feature_flags(source):
    text=(Path(source)/'src/Makefile.global').read_text()
    values={m.group(1):m.group(2).strip()for m in re.finditer(r'^([a-z0-9_]+)\s*=\s*([^\n]*)',text,re.MULTILINE)}
    flags=[]
    for macro in ['USE_SPQ','ZSTD_STATIC_LINKING_ONLY']:
        if not re.search(r'^override CPPFLAGS\s*[:+]?=.*-D'+macro+r'\b',text,re.MULTILINE):
            raise RuntimeError('Native unconditional macro contract changed: '+macro)
        flags.append('-D'+macro)
    for option,macro in [('enable_x86_rdtscp','ENABLE_X86_RDTSCP'),('enable_htap','ENABLE_HTAP'),
            ('with_openeuler_os','WITH_OPENEULER_OS'),('with_openeuler_major','OPENEULER_MAJOR'),
            ('with_obs','ENABLE_OBS'),('with_openssl3','ENABLE_OPENSSL3'),('enable_libnet','USE_LIBNET')]:
        if values.get(option)not in ['yes','no']:raise RuntimeError('Unreviewed native feature value: '+option)
        if values[option]=='yes':flags.append('-D'+macro)
    return flags

EXPRESSIONS=[
    'sizeof(t_thrd)',
    '(long)&((knl_thrd_context*)0)->log_cxt.PG_exception_stack',
    '(long)&((knl_thrd_context*)0)->log_cxt.exception_stack',
    '(long)&((knl_thrd_context*)0)->log_cxt.error_context_stack',
    '(long)&((knl_thrd_context*)0)->int_cxt.InterruptHoldoffCount',
    '(long)&((knl_thrd_context*)0)->int_cxt.CritSectionCount',
    'sizeof(knl_t_log_context)',
    'sizeof(knl_session_context)',
    '(long)&((knl_session_context*)0)->attr',
    '(long)&((knl_session_context*)0)->plsql_cxt',
]

def check_layout(native,module,evidence):
    evidence=Path(evidence);layouts={}
    for name,binary in [('native',native),('module',module)]:
        args=['gdb','--quiet','--batch',str(binary)]
        for expression in EXPRESSIONS:args+=['-ex','p '+expression]
        run=subprocess.run(args,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=90)
        (evidence/(name+'-layout.log')).write_text(run.stdout)
        numbers=re.findall(r'^\$\d+ = ([0-9]+)\s*$',run.stdout,re.MULTILINE)
        if run.returncode or len(numbers)!=len(EXPRESSIONS):raise RuntimeError('Cannot prove every diagnostic layout: '+name)
        layouts[name]={k:int(v)for k,v in zip(EXPRESSIONS,numbers)}
    result=dict(status='pass'if layouts['native']==layouts['module']else'fail',layouts=layouts)
    (evidence/'abi-layout.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    if result['status']!='pass':raise RuntimeError('Diagnostic/native struct layouts differ; module must not be loaded')
    return result
