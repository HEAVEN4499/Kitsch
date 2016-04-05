package kitsch

/**
 * Created by wulicheng on 16/3/25.
 */
case class KitschConf(var name: String = "Kitsch",
                      var mode: String = Kitsch.Mod.Master,
                 var localhost: String = "127.0.0.1",
                      var port: Int = 2552) {
  def setName(n: String) = {
    name = n
    this
  }

  def setMode(mod: String) = {
    mode = mod
    this
  }

  def setLocalhost(ip: String) = {
    localhost = ip
    this
  }

  def setPort(p: Int) = {
    port = p
    this
  }
}
