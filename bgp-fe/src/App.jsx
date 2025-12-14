import { useState, useEffect, useRef } from 'react'
import './App.css'

function App() {
  const [isScriptLoaded, setIsScriptLoaded] = useState(false)
  const bgplayRef = useRef(null)

  // BGPlay 파라미터 상태
  const [params, setParams] = useState({
    resource: '193.0.0.0/21',
    starttime: '',
    endtime: '',
    rrcs: '',
    ignoreReannouncements: false
  })

  // BGPlay 위젯 옵션
  const [options, setOptions] = useState({
    width: 1100,
    height: 800
  })

  // BGPlay 스크립트 로드
  useEffect(() => {
    const script = document.createElement('script')
    script.src = 'https://bgplay.massimocandela.com/bgplay/widget/bgplayjs-main-widget.js'
    script.async = true
    script.onload = () => setIsScriptLoaded(true)
    document.body.appendChild(script)
  }, [])

  // 파라미터 변경 핸들러
  const handleParamChange = (key, value) => {
    setParams(prev => ({ ...prev, [key]: value }))
  }

  // 옵션 변경 핸들러
  const handleOptionChange = (key, value) => {
    setOptions(prev => ({ ...prev, [key]: value }))
  }

  // BGPlay 로드
  const loadBGPlay = () => {
    if (!isScriptLoaded) {
      alert('BGPlay 스크립트 로딩 중입니다.')
      return
    }

    if (!params.resource.trim()) {
      alert('Resource를 입력해주세요.')
      return
    }

    // 기존 인스턴스 제거
    if (bgplayRef.current) {
      bgplayRef.current.innerHTML = ''
    }

    // 파라미터 객체 구성 (빈 값 제외)
    const bgplayParams = {
      resource: params.resource.trim()
    }

    if (params.starttime) {
      bgplayParams.starttime = Math.floor(new Date(params.starttime).getTime() / 1000)
    }
    if (params.endtime) {
      bgplayParams.endtime = Math.floor(new Date(params.endtime).getTime() / 1000)
    }
    if (params.rrcs.trim()) {
      bgplayParams.rrcs = params.rrcs.trim()
    }
    if (params.ignoreReannouncements) {
      bgplayParams.ignoreReannouncements = true
    }

    // BGPlay 위젯 초기화
    window.BGPlayWidget(
      'BGPlay',   // Version type (classic)
      'bgplay',   // DOM element ID to populate
      {
        width: options.width,
        height: options.height
      },
      bgplayParams
    )

    console.log('BGPlay 로드:', { options, params: bgplayParams })
  }

  return (
    <div className="app">
      <h1>BGPlay 연습 프로젝트</h1>

      <div className="controls">
        <fieldset>
          <legend>Widget Options</legend>
          <div className="field">
            <label>Width</label>
            <input
              type="number"
              value={options.width}
              onChange={(e) => handleOptionChange('width', Number(e.target.value))}
            />
          </div>
          <div className="field">
            <label>Height</label>
            <input
              type="number"
              value={options.height}
              onChange={(e) => handleOptionChange('height', Number(e.target.value))}
            />
          </div>
        </fieldset>

        <fieldset>
          <legend>BGPlay Parameters</legend>
          <div className="field">
            <label>Resource *</label>
            <input
              type="text"
              value={params.resource}
              onChange={(e) => handleParamChange('resource', e.target.value)}
              placeholder="예: 193.0.0.0/21 또는 AS3333"
            />
            <small>Comma-separated resources to monitor</small>
          </div>
          <div className="field">
            <label>Start Time</label>
            <input
              type="datetime-local"
              value={params.starttime}
              onChange={(e) => handleParamChange('starttime', e.target.value)}
            />
          </div>
          <div className="field">
            <label>End Time</label>
            <input
              type="datetime-local"
              value={params.endtime}
              onChange={(e) => handleParamChange('endtime', e.target.value)}
            />
          </div>
          <div className="field">
            <label>RRCs</label>
            <input
              type="text"
              value={params.rrcs}
              onChange={(e) => handleParamChange('rrcs', e.target.value)}
              placeholder="예: 1,2,3,4"
            />
            <small>A list of Route Collectors</small>
          </div>
          <div className="field checkbox">
            <label>
              <input
                type="checkbox"
                checked={params.ignoreReannouncements}
                onChange={(e) => handleParamChange('ignoreReannouncements', e.target.checked)}
              />
              Ignore Reannouncements
            </label>
          </div>
        </fieldset>

        <button onClick={loadBGPlay} disabled={!isScriptLoaded}>
          {isScriptLoaded ? 'BGPlay 로드' : '스크립트 로딩...'}
        </button>
      </div>

      <div id="bgplay" ref={bgplayRef}></div>
    </div>
  )
}

export default App
