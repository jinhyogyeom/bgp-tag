import { useState, useEffect, useRef, useCallback } from 'react'
import './App.css'

function App() {
  const [isScriptLoaded, setIsScriptLoaded] = useState(false)
  const bgplayRef = useRef(null)
  const [messages, setMessages] = useState([])
  const [inputMessage, setInputMessage] = useState('')
  const [chatHistory, setChatHistory] = useState([
    { id: 1, title: 'BGP 분석 세션 1', lastMessage: '193.0.0.0/21 분석 요청', timestamp: '2024-03-01' },
    { id: 2, title: 'BGP 분석 세션 2', lastMessage: 'AS3333 라우팅 분석', timestamp: '2024-03-02' }
  ])
  const [selectedChat, setSelectedChat] = useState(null)

  // BGPlay 로드 함수
  const loadBGPlay = useCallback((params) => {
    if (!isScriptLoaded && !window.BGPlayWidget) {
      setTimeout(() => loadBGPlay(params), 100)
      return
    }

    // 기존 인스턴스 제거
    if (bgplayRef.current) {
      bgplayRef.current.innerHTML = ''
    }

    // BGPlay 위젯 초기화 (index.html 코드 그대로)
    if (window.BGPlayWidget) {
      window.BGPlayWidget(
        'BGPlay', // Version type (classic)
        'bgplay',  // DOM element ID to populate
        {
          width: 1100,
          height: 800
        },
        params
      )
    }
  }, [isScriptLoaded])

  // BGPlay 스크립트 로드
  useEffect(() => {
    // BGPLAY_PROJECT_URL 설정
    window.BGPLAY_PROJECT_URL = "https://bgplay.massimocandela.com/bgplay/";
    
    // require.js 로드
    const requireScript = document.createElement('script')
    requireScript.src = 'https://bgplay.massimocandela.com/bgplay/lib/require.js'
    requireScript.async = true
    
    // BGPlay 위젯 스크립트 로드
    const bgplayScript = document.createElement('script')
    bgplayScript.src = 'https://bgplay.massimocandela.com/bgplay/widget/bgplayjs-main-widget.js'
    bgplayScript.async = true
    
    requireScript.onload = () => {
      bgplayScript.onload = () => {
        setIsScriptLoaded(true)
        // 초기 BGPlay 로드
        loadBGPlay({
          resource: "193.0.0.0/21",
          starttime: 1709251200,
          endtime: 1710460800,
          rrcs: "1",
          ignoreReannouncements: true
        })
      }
      document.body.appendChild(bgplayScript)
    }
    
    document.body.appendChild(requireScript)
  }, [loadBGPlay])

  // 메시지 전송
  const handleSendMessage = () => {
    if (!inputMessage.trim()) return

    const newMessage = {
      id: messages.length + 1,
      text: inputMessage,
      sender: 'user',
      timestamp: new Date().toLocaleTimeString()
    }

    setMessages([...messages, newMessage])
    setInputMessage('')

    // BGPlay 관련 키워드 감지
    if (inputMessage.toLowerCase().includes('bgplay') || 
        inputMessage.toLowerCase().includes('bgp') ||
        inputMessage.match(/\d+\.\d+\.\d+\.\d+\/\d+/)) {
      // BGPlay 파라미터 추출 (간단한 예시)
      const ipMatch = inputMessage.match(/(\d+\.\d+\.\d+\.\d+\/\d+)/)
      if (ipMatch) {
        loadBGPlay({
          resource: ipMatch[1],
          starttime: 1709251200,
          endtime: 1710460800,
          rrcs: "1",
          ignoreReannouncements: true
        })
      }
    }
  }

  // Enter 키 처리
  const handleKeyPress = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      handleSendMessage()
    }
  }

  return (
    <div className="app-container">
      {/* 좌측: 채팅방 메모리 */}
      <div className="sidebar">
        <div className="sidebar-header">
          <h2>채팅 기록</h2>
          <button className="new-chat-btn">+ 새 채팅</button>
        </div>
        <div className="chat-list">
          {chatHistory.map(chat => (
            <div 
              key={chat.id} 
              className={`chat-item ${selectedChat === chat.id ? 'active' : ''}`}
              onClick={() => setSelectedChat(chat.id)}
            >
              <div className="chat-title">{chat.title}</div>
              <div className="chat-preview">{chat.lastMessage}</div>
              <div className="chat-timestamp">{chat.timestamp}</div>
            </div>
          ))}
        </div>
      </div>

      {/* 중앙: 채팅 */}
      <div className="chat-area">
        <div className="chat-header">
          <h3>BGP 분석 어시스턴트</h3>
        </div>
        <div className="messages-container">
          {messages.length === 0 ? (
            <div className="empty-state">
              <p>BGP 분석을 시작하세요. 예: "193.0.0.0/21에 대한 BGPlay를 보여줘"</p>
            </div>
          ) : (
            messages.map(msg => (
              <div key={msg.id} className={`message ${msg.sender}`}>
                <div className="message-content">{msg.text}</div>
                <div className="message-time">{msg.timestamp}</div>
              </div>
            ))
          )}
        </div>
        <div className="chat-input-container">
          <textarea
            className="chat-input"
            value={inputMessage}
            onChange={(e) => setInputMessage(e.target.value)}
            onKeyPress={handleKeyPress}
            placeholder="메시지를 입력하세요..."
            rows="1"
          />
          <button 
            className="send-button"
            onClick={handleSendMessage}
            disabled={!inputMessage.trim()}
          >
            전송
          </button>
        </div>
      </div>

      {/* 우측: 시각화 영역 */}
      <div className="visualization-area">
        <div className="visualization-header">
          <h3>시각화</h3>
        </div>
        <div className="visualization-content">
          <div id="bgplay" ref={bgplayRef}></div>
        </div>
      </div>
    </div>
  )
}

export default App
