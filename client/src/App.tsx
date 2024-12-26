import React from 'react'
import './index.scss'
import { Outlet, Route, Routes } from 'react-router-dom'
import Login from './pages/Login'
import Register from './pages/Register'

const App = () => {

  return <>
    <Routes>
      <Route path='/' element={<><Outlet /></>}>
        <Route index element={<Login />} />
        <Route path='sign-up' element={<Register />} />
      </Route>
    </Routes>
  </>
}

export default App
