#class_character_replacer.py
class CharacterReplacer:
    @staticmethod
    def replace_characters(text):
        replacements = {
            'á': '&aacute;',
            'é': '&eacute;',
            'í': '&iacute;',
            'ó': '&oacute;',
            'ú': '&uacute;',
            'ñ': '&ntilde;',
            'Á': '&Aacute;',
            'É': '&Eacute;',
            'Í': '&Iacute;',
            'Ó': '&Oacute;',
            'Ú': '&Uacute;',
            'Ñ': '&Ntilde;'
        }

        for char, replacement in replacements.items():
            text = text.replace(char, replacement)

        return text