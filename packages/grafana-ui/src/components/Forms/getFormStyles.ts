import { stylesFactory } from '../../themes';
import { GrafanaTheme } from '@grafana/data';
import { getLabelStyles } from './Label';
import { getLegendStyles } from './Legend';
import { getFieldValidationMessageStyles } from './FieldValidationMessage';
import { getButtonStyles, ButtonVariant } from './Button';
import { ButtonSize } from '../Button/types';
import { getInputStyles } from './Input/Input';
import { getSwitchStyles } from './Switch';

export const getFormStyles = stylesFactory(
  (theme: GrafanaTheme, options: { variant: ButtonVariant; size: ButtonSize; invalid: boolean }) => {
    return {
      ...getLabelStyles(theme),
      ...getLegendStyles(theme),
      ...getFieldValidationMessageStyles(theme),
      ...getButtonStyles({
        theme,
        variant: options.variant,
        size: options.size,
      }),
      ...getInputStyles({ theme, invalid: options.invalid }),
      ...getSwitchStyles(theme),
    };
  }
);
