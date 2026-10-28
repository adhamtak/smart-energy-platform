import { createContext, useContext, useEffect, useMemo, useState } from 'react'

const DICT = {
  en: {
    'app.name': 'Smart Energy',
    'lang.other': 'العربية',
    'common.loading': 'Loading…',

    'login.heroTitle': 'Your meter, in plain numbers.',
    'login.heroText': 'See what you use, when you use it, and whether your voltage held up.',
    'login.demo': 'Demo data. Readings are synthetic.',
    'login.title': 'Sign in',
    'login.sub': 'Use the account you were given to see your meter.',
    'login.username': 'Username',
    'login.password': 'Password',
    'login.submit': 'Sign in',
    'login.busy': 'Signing in…',
    'err.Wrong username or password': 'Wrong username or password',

    'top.admin': 'admin',
    'top.signOut': 'Sign out',
    'meter.label': 'Meter',
    'meter.show': 'Show meter',

    'fact.customer': 'Customer',
    'fact.phase': 'Phase',
    'fact.transformer': 'Transformer',
    'fact.status': 'Status',
    'hint.transformer': 'The equipment in your area that feeds your meter.',

    'sum.title': 'At a glance',
    'sum.usage': 'You used {kwh} kWh in total, about {avg} kWh a day.',
    'sum.usageShort': 'You used {kwh} kWh in total.',
    'sum.peak': 'Your highest use was {kw} kW, on {time}.',
    'sum.voltageOk': 'Your voltage stayed steady. The lowest reading was {min} V.',
    'sum.voltageLow': 'Your voltage dropped below {limit} V in {n} of {total} readings. Low voltage can strain appliances.',
    'sum.noIncidents': 'No incidents were recorded on your meter or its transformer.',
    'sum.incidents': 'We detected {n} incident(s) on your meter or its transformer. See the list below.',

    'hero.label': 'Total consumption',
    'hero.range': '{from} to {to}',
    'hero.noReadings': 'No readings in this period',
    'hero.meta': '{n} readings · times in {tz}',
    'unit.kwhLong': 'kilowatt-hours',

    'volt.label': 'Voltage',
    'volt.stable': 'Stable',
    'volt.dipped': 'Dipped below limit',
    'volt.lowest': 'Lowest reading',
    'volt.ok': 'Every reading stayed above {limit} V. Average {avg} V.',
    'volt.low': 'Below {limit} V in {n} of {total} readings. Average {avg} V.',
    'hint.voltage': 'The strength of the supply reaching your meter. Dips can strain appliances.',

    'stat.avgDay': 'Average per day',
    'hint.avgDay': 'The average energy you use in one day.',
    'stat.peak': 'Peak load',
    'hint.peak': 'The most power you drew at one moment. Running many appliances together pushes it up.',
    'stat.pf': 'Average power factor',
    'hint.pf': 'How efficiently your appliances use power. The closer to 1, the better.',

    'chart.consumption': 'Consumption',
    'chart.view': 'Chart view',
    'tab.hour': 'Per hour',
    'tab.day': 'Per day',
    'tab.load': 'Load',
    'note.hour': 'Energy used in each hour',
    'note.day': 'Energy used each day',
    'note.load': 'Power drawn at each reading',
    'chart.voltage': 'Voltage',
    'chart.voltageNote': 'Shaded area is below {limit} V',
    'chart.limit': 'Limit {limit} V',
    'chart.pf': 'Power factor',
    'chart.pfNote': '1.0 means no power is wasted',

    'inc.title': 'Incidents',
    'inc.empty': 'No incidents on this meter or its transformer.',
    'inc.detected': 'Detected',
    'inc.severity': 'Severity',
    'inc.type': 'Type',
    'inc.asset': 'Asset',
    'inc.status': 'Status',
    'inc.observed': 'Observed',
    'inc.threshold': 'Threshold',

    'read.title': 'Latest readings',
    'read.csv': 'Download CSV',
    'read.time': 'Time',
    'read.voltage': 'Voltage (V)',
    'read.load': 'Load (kW)',
    'read.energy': 'Energy (kWh)',
    'read.pf': 'Power factor',

    'chat.open': 'Ask about your meter',
    'chat.title': 'Assistant',
    'chat.close': 'Close',
    'chat.hello': 'Ask me about your readings: voltage, usage, peak load or incidents.',
    'chat.placeholder': 'Type your question…',
    'chat.send': 'Send',
    'chat.thinking': 'Thinking…',
    'chat.error': 'Could not get an answer. Check that the backend is running and try again.',
    'chat.basic': 'Quick answer from your data (basic mode)',
    'chat.note': 'Answers come from your meter data.',
    'chat.s1': 'When was my voltage highest?',
    'chat.s2': 'What time do I use the most electricity?',
    'chat.s3': 'Which day used the most?',
    'chat.s4': 'Any incidents on my meter?',

    'err.notFoundTitle': 'Meter not found',
    'err.notFound': 'There is no meter named {id} in the data. Check the ID and try again.',
    'err.noAccessTitle': 'No access',
    'err.noAccess': "Your account can't view {id}.",
    'err.genericTitle': 'Something went wrong',

    'cust.RESIDENTIAL': 'Residential',
    'cust.COMMERCIAL': 'Commercial',
    'cust.INDUSTRIAL': 'Industrial',
    'ph.SINGLE_PHASE': 'Single phase',
    'ph.THREE_PHASE': 'Three phase',
    'ms.ACTIVE': 'Active',
    'ms.INACTIVE': 'Inactive',
    'sev.LOW': 'Low',
    'sev.MEDIUM': 'Medium',
    'sev.HIGH': 'High',
    'sev.CRITICAL': 'Critical',
    'st.OPEN': 'Open',
    'st.INVESTIGATING': 'Investigating',
    'st.RESOLVED': 'Resolved',
    'an.TRANSFORMER_OVERLOAD': 'Transformer overload',
    'an.VOLTAGE_DROP': 'Voltage drop',
    'an.IRREGULAR_CONSUMPTION': 'Irregular consumption',
  },

  ar: {
    'app.name': 'الطاقة الذكية',
    'lang.other': 'English',
    'common.loading': 'جارٍ التحميل…',

    'login.heroTitle': 'عدّادك بأرقام بسيطة.',
    'login.heroText': 'اعرف كم تستهلك ومتى، وهل ظلّ الجهد عندك ثابتًا.',
    'login.demo': 'بيانات تجريبية. القراءات مُولَّدة اصطناعيًا.',
    'login.title': 'تسجيل الدخول',
    'login.sub': 'استخدم الحساب الذي حصلت عليه لعرض عدّادك.',
    'login.username': 'اسم المستخدم',
    'login.password': 'كلمة المرور',
    'login.submit': 'دخول',
    'login.busy': 'جارٍ الدخول…',
    'err.Wrong username or password': 'اسم المستخدم أو كلمة المرور غير صحيحة',

    'top.admin': 'مشرف',
    'top.signOut': 'تسجيل الخروج',
    'meter.label': 'العدّاد',
    'meter.show': 'عرض العدّاد',

    'fact.customer': 'نوع العميل',
    'fact.phase': 'الطور',
    'fact.transformer': 'المحوّل',
    'fact.status': 'الحالة',
    'hint.transformer': 'المعدّة في منطقتك التي تغذّي عدّادك بالكهرباء.',

    'sum.title': 'ملخص سريع',
    'sum.usage': 'استهلكت إجمالًا {kwh} كيلوواط‑ساعة، أي نحو {avg} كيلوواط‑ساعة يوميًا.',
    'sum.usageShort': 'استهلكت إجمالًا {kwh} كيلوواط‑ساعة.',
    'sum.peak': 'أعلى استهلاك لك كان {kw} كيلوواط، بتاريخ {time}.',
    'sum.voltageOk': 'ظلّ الجهد عندك ثابتًا. أقل قراءة كانت {min} فولت.',
    'sum.voltageLow': 'انخفض الجهد عن {limit} فولت في {n} من أصل {total} قراءة. الجهد المنخفض قد يرهق الأجهزة.',
    'sum.noIncidents': 'لم تُسجَّل أي تنبيهات على عدّادك أو محوّله.',
    'sum.incidents': 'رصدنا {n} تنبيه على عدّادك أو محوّله. التفاصيل في القائمة أدناه.',

    'hero.label': 'إجمالي الاستهلاك',
    'hero.range': 'من {from} إلى {to}',
    'hero.noReadings': 'لا توجد قراءات في هذه الفترة',
    'hero.meta': '{n} قراءة · التوقيت: {tz}',
    'unit.kwhLong': 'كيلوواط ساعة',

    'volt.label': 'الجهد',
    'volt.stable': 'مستقر',
    'volt.dipped': 'انخفض عن الحد',
    'volt.lowest': 'أقل قراءة',
    'volt.ok': 'ظلّت كل القراءات أعلى من {limit} فولت. المتوسط {avg} فولت.',
    'volt.low': 'أقل من {limit} فولت في {n} من أصل {total} قراءة. المتوسط {avg} فولت.',
    'hint.voltage': 'قوة التيار الواصل إلى عدّادك. انخفاضه قد يرهق الأجهزة.',

    'stat.avgDay': 'المتوسط اليومي',
    'hint.avgDay': 'متوسط الطاقة التي تستهلكها في اليوم الواحد.',
    'stat.peak': 'أعلى حمل',
    'hint.peak': 'أكبر قدرة سحبتها في لحظة واحدة. تشغيل عدة أجهزة معًا يرفعها.',
    'stat.pf': 'متوسط عامل القدرة',
    'hint.pf': 'مدى كفاءة استخدام أجهزتك للقدرة. كلما اقترب من 1 كان أفضل.',

    'chart.consumption': 'الاستهلاك',
    'chart.view': 'طريقة عرض الرسم',
    'tab.hour': 'كل ساعة',
    'tab.day': 'كل يوم',
    'tab.load': 'الحمل',
    'note.hour': 'الطاقة المستهلكة في كل ساعة',
    'note.day': 'الطاقة المستهلكة في كل يوم',
    'note.load': 'القدرة المسحوبة عند كل قراءة',
    'chart.voltage': 'الجهد',
    'chart.voltageNote': 'المنطقة المظللة أقل من {limit} فولت',
    'chart.limit': 'الحد {limit} فولت',
    'chart.pf': 'عامل القدرة',
    'chart.pfNote': 'القيمة 1.0 تعني عدم إهدار أي قدرة',

    'inc.title': 'التنبيهات',
    'inc.empty': 'لا توجد تنبيهات على هذا العدّاد أو محوّله.',
    'inc.detected': 'وقت الرصد',
    'inc.severity': 'الخطورة',
    'inc.type': 'النوع',
    'inc.asset': 'الجهاز',
    'inc.status': 'الحالة',
    'inc.observed': 'القيمة المرصودة',
    'inc.threshold': 'الحد',

    'read.title': 'آخر القراءات',
    'read.csv': 'تنزيل CSV',
    'read.time': 'الوقت',
    'read.voltage': 'الجهد (V)',
    'read.load': 'الحمل (kW)',
    'read.energy': 'الطاقة (kWh)',
    'read.pf': 'عامل القدرة',

    'chat.open': 'اسأل عن عدّادك',
    'chat.title': 'المساعد',
    'chat.close': 'إغلاق',
    'chat.hello': 'اسألني عن قراءاتك: الجهد والاستهلاك وأعلى حمل والتنبيهات.',
    'chat.placeholder': 'اكتب سؤالك…',
    'chat.send': 'إرسال',
    'chat.thinking': 'جارٍ التفكير…',
    'chat.error': 'تعذّر الحصول على إجابة. تأكد أن الـ backend يعمل وحاول مرة أخرى.',
    'chat.basic': 'إجابة سريعة من بياناتك (الوضع الأساسي)',
    'chat.note': 'الإجابات مصدرها بيانات عدّادك.',
    'chat.s1': 'متى كان الجهد الأعلى عندي؟',
    'chat.s2': 'في أي وقت أستهلك أكثر كهرباء؟',
    'chat.s3': 'أي يوم كان استهلاكه الأعلى؟',
    'chat.s4': 'هل توجد تنبيهات على عدّادي؟',

    'err.notFoundTitle': 'العدّاد غير موجود',
    'err.notFound': 'لا يوجد عدّاد باسم {id} في البيانات. تأكد من الرقم وحاول مرة أخرى.',
    'err.noAccessTitle': 'لا توجد صلاحية',
    'err.noAccess': 'حسابك لا يملك صلاحية عرض {id}.',
    'err.genericTitle': 'حدث خطأ ما',

    'cust.RESIDENTIAL': 'سكني',
    'cust.COMMERCIAL': 'تجاري',
    'cust.INDUSTRIAL': 'صناعي',
    'ph.SINGLE_PHASE': 'طور واحد',
    'ph.THREE_PHASE': 'ثلاثة أطوار',
    'ms.ACTIVE': 'يعمل',
    'ms.INACTIVE': 'متوقف',
    'sev.LOW': 'منخفضة',
    'sev.MEDIUM': 'متوسطة',
    'sev.HIGH': 'عالية',
    'sev.CRITICAL': 'حرجة',
    'st.OPEN': 'مفتوح',
    'st.INVESTIGATING': 'قيد الفحص',
    'st.RESOLVED': 'تم الحل',
    'an.TRANSFORMER_OVERLOAD': 'حمل زائد على المحوّل',
    'an.VOLTAGE_DROP': 'هبوط في الجهد',
    'an.IRREGULAR_CONSUMPTION': 'استهلاك غير معتاد',
  },
}

const I18nContext = createContext(null)

const initialLang = () => {
  try {
    const saved = localStorage.getItem('lang')
    if (saved === 'en' || saved === 'ar') return saved
  } catch {
    /* storage unavailable */
  }
  return typeof navigator !== 'undefined' && navigator.language?.toLowerCase().startsWith('ar') ? 'ar' : 'en'
}

export function I18nProvider({ children }) {
  const [lang, setLang] = useState(initialLang)

  useEffect(() => {
    document.documentElement.lang = lang
    document.documentElement.dir = lang === 'ar' ? 'rtl' : 'ltr'
    try {
      localStorage.setItem('lang', lang)
    } catch {
      /* ignore */
    }
  }, [lang])

  const value = useMemo(() => {
    const raw = (key) => DICT[lang][key] ?? DICT.en[key]
    const fill = (text, vars) => (vars ? text.replace(/\{(\w+)\}/g, (_, k) => (vars[k] ?? '')) : text)
    const t = (key, vars) => fill(raw(key) ?? key, vars)
    // Translate a value that comes from the data (e.g. RESIDENTIAL); fall back to a readable version of it.
    const tv = (group, v) => {
      if (v == null || v === '') return ''
      const text = raw(`${group}.${String(v).toUpperCase()}`)
      if (text) return text
      const plain = String(v).toLowerCase().replace(/_/g, ' ')
      return plain.charAt(0).toUpperCase() + plain.slice(1)
    }
    return { lang, setLang, t, tv, dir: lang === 'ar' ? 'rtl' : 'ltr' }
  }, [lang])

  return <I18nContext.Provider value={value}>{children}</I18nContext.Provider>
}

export const useI18n = () => useContext(I18nContext)

export function LangToggle({ className = 'ghost' }) {
  const { lang, setLang, t } = useI18n()
  return (
    <button type="button" className={className} onClick={() => setLang(lang === 'ar' ? 'en' : 'ar')} lang={lang === 'ar' ? 'en' : 'ar'}>
      {t('lang.other')}
    </button>
  )
}
