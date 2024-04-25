using System;
using System.IO;
using System.Xml.Serialization;
using System.Reflection;
using System.Xml.Linq;

namespace DevelopmentActivity.Producer.Shared
{
    public static class ConfigManager
    {
        public static void LoadConfiguration(Type configType, string elementName)
        {
            var doc = XDocument.Load(Path.Combine(Path.GetDirectoryName(Assembly.GetEntryAssembly().Location), "config.xml"));

            XElement root = doc.Element("Config").Element(elementName);

            if (root != null)
            {
                foreach (var prop in configType.GetProperties(BindingFlags.Static | BindingFlags.Public))
                {
                    var element = root.Element(prop.Name);
                    if (element != null)
                    {
                        try
                        {
                            // Convert the XElement's value to the property's type and set the value
                            object value = Convert.ChangeType(element.Value, prop.PropertyType);
                            prop.SetValue(null, value);
                        }
                        catch (Exception ex)
                        {
                            Logger.Log("ConfigManager", $"Error setting property {prop.Name} from XML configuration: {ex.Message}");
                        }
                    }
                    else
                    {
                        Logger.Log("ConfigManager", $"ConfigManager: Configuration element '{prop.Name}' not found in XML for '{elementName}'.");
                    }
                }
            }
            else
            {
                Logger.Log("ConfigManager", $"ConfigManager: Configuration section '{elementName}' not found in XML.");
            }
        }
    }
}
